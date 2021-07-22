import argparse
import asyncio
import json
import logging
import shlex
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from io import StringIO
import random

import aiohttp
import aiosqlite
import click
import pytz
import yaml
from cron_descriptor import get_description
from croniter import croniter
from sanic import Sanic
from sanic import response
from sanic.exceptions import Forbidden

app = Sanic(__name__)


async def create_schema(conn):
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS schedule (username TEXT PRIMARY KEY,
        schedule TEXT DEFAULT '0 9 * * 1-5',
        window INTEGER DEFAULT 3,
        registered TIMESTAMP DEFAULT (DATETIME('now')))"""
    )
    await conn.execute(
        """CREATE TABLE IF NOT EXISTS messages (username TEXT,
        timestamp TIMESTAMP,
        message TEXT)"""
    )


parser = argparse.ArgumentParser(prog="phb")
subparsers = parser.add_subparsers(dest="subparser")
parser_schedule = subparsers.add_parser("schedule", help="Manage standup schedules")
parser_schedule.add_argument("--username", help="Username (default sender)")
parser_schedule.add_argument("--schedule", help='Set schedule (e.g. "0 9 * * 1-5")')
parser_schedule.add_argument(
    "--window", type=int, help="Set window in which to send the standup message"
)

parser_schedules = subparsers.add_parser("schedules", help="Manage standup schedules")

parser_121 = subparsers.add_parser("121", help="Have a professional one-on-one session")

parser_standup = subparsers.add_parser("standup", help="Send standup message")
parser_standup.add_argument("--username", help="Username (default sender)")
parser_standup.add_argument(
    "--date",
    help="Date in YYYY-MM-DD (will update a message on this date if passed, "
    "fetch if no text)",
    type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
)
parser_standup.add_argument("text", nargs="*", help="Standup message")

parser_stats = subparsers.add_parser("stats", help="Standup stats")
parser_stats.add_argument("--username", help="Username (default sender)")

subparsers.add_parser("help")


async def set_schedule(username, conn, schedule=None, window=None):
    if schedule:
        # validate
        croniter(schedule)
        await conn.execute(
            "INSERT INTO schedule (username, schedule) VALUES (?, ?) "
            "ON CONFLICT (username) DO UPDATE SET schedule=excluded.schedule",
            parameters=(username, schedule),
        )
    if window:
        await conn.execute(
            "INSERT INTO schedule (username, window) VALUES (?, ?) "
            "ON CONFLICT (username) DO UPDATE SET window=excluded.window",
            parameters=(username, window),
        )


async def get_schedule(username, conn):
    async with conn.execute(
        "SELECT schedule, window FROM schedule WHERE username = ?",
        parameters=(username,),
    ) as cur:
        return await cur.fetchone()


async def manage_schedule(context, conn, app):
    username = context["username"]
    if context["schedule"] or context["window"]:
        await set_schedule(
            username, conn, context.get("schedule"), context.get("window")
        )
        schedule, window = await get_schedule(username, conn)
        print(
            "New schedule for user %s: %s (within %s hour(s))"
            % (username, get_description(schedule), window)
        )
        regenerate_reminder_for_user(
            schedule,
            username,
            window,
            datetime.now(pytz.timezone(context["timezone"])),
            app,
        )
    else:
        row = await get_schedule(username, conn)
        if not row:
            print(
                "User %s has no schedule. Set one with e.g. "
                '@phb schedule --username %s --schedule "0 9 * * 1-5" --window 3'
                % (username, username)
            )
            return
        schedule, window = row
        print(
            "User %s's schedule: %s (within %d hour(s))"
            % (username, get_description(schedule), window)
        )


async def get_all_schedules(context, conn):
    async with conn.execute(
        "SELECT username, schedule, window FROM schedule",
    ) as cur:
        schedules = await cur.fetchall()
    if not schedules:
        print("No users registered.")
    print("Standup schedules: ")
    for username, schedule, window in schedules:
        print(
            "  * %s: %s (within %d hour(s))"
            % (username, get_description(schedule), window)
        )


async def get_standup_message(username, date, conn):
    async with conn.execute(
        "SELECT message, timestamp FROM messages "
        "WHERE username = ? AND date(timestamp) = date(?)",
        parameters=(username, date.astimezone(pytz.utc)),
    ) as cur:
        return await cur.fetchone()


async def get_message_history(username, conn):
    async with conn.execute(
        "SELECT timestamp, message FROM messages "
        "WHERE username = ? ORDER BY timestamp ASC",
        parameters=(username,),
    ) as cur:
        return await cur.fetchall()


async def set_standup_message(username, timestamp, text, conn):
    await conn.execute(
        "INSERT INTO messages (username, timestamp, message) VALUES (?, ?, ?)",
        parameters=(username, timestamp.astimezone(pytz.utc), text),
    )


async def update_standup_message(username, timestamp, text, conn):
    await conn.execute(
        "UPDATE messages SET message = ? WHERE username = ? AND date(timestamp) = date(?)",
        parameters=(text, username, timestamp),
    )


async def standup(context, conn):
    username = context["username"]
    text = " ".join(context["text"])
    tz = pytz.timezone(context["timezone"])

    if context["date"]:
        update_date = midnight(pytz.utc.localize(context["date"]))
        previous_message = await get_standup_message(username, update_date, conn)

        if not text:
            if not previous_message:
                print("No status update on %s" % update_date.strftime("%Y-%m-%d"))
            else:
                print(
                    "%s: %s" % (update_date.strftime("%Y-%m-%d"), previous_message[0])
                )
            return

        if not previous_message:
            print(
                "Nice try, you didn't submit a message on %s!"
                % update_date.strftime("%Y-%m-%d")
            )
            return
        await update_standup_message(username, previous_message[1], text, conn)
        print(
            "@%s Updated previous standup message for %s"
            % (username, update_date.strftime("%Y-%m-%d"))
        )
        return

    now = datetime.now(tz)
    previous_message = await get_standup_message(username, now, conn)
    if previous_message:
        await update_standup_message(username, previous_message[1], text, conn)
        print(
            "@%s Updated previous standup message for today (sent at %s)"
            % (username, previous_message[1])
        )
        return

    reply = "@%s Congrats!" % username
    row = await get_schedule(username, conn)
    if row:
        schedule, window = row
        ct = croniter(schedule, start_time=midnight(now))
        deadline = ct.get_next(ret_type=datetime) + timedelta(hours=window)
        logging.info("Deadline was %s", deadline)
        if deadline < now:
            reply += " (you're late though, see @phb schedule)"
    else:
        reply += " (feel free to set a schedule with @phb schedule)"
    await set_standup_message(username, now, text, conn)
    print(reply)


def midnight(dt):
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


async def stats(context, conn):
    username = context["username"]
    row = await get_schedule(username, conn)
    local_tz = pytz.timezone(context["timezone"])

    if not row:
        print("%s has no schedule set, set one with @phb schedule." % username)
        return

    schedule, window = row
    print("Stats for %s (all times are in %s):" % (username, local_tz))
    print("Schedule: %s (within %d hour(s))" % (get_description(schedule), window))

    message_history = [
        (pytz.utc.localize(ts).astimezone(local_tz), m)
        for ts, m in await get_message_history(username, conn)
    ]
    logging.info("message history %s", message_history)
    if not message_history:
        print("No standup updates yet!")
        return

    current_streak = 0
    current_streak_start = None
    longest_streak = 0
    longest_streak_start = None

    ct = croniter(schedule, start_time=midnight(message_history[0][0]))
    ci = iter(ct.all_next(ret_type=datetime))

    schedule_ts = next(ci)
    today = midnight(datetime.now(local_tz))

    for message_ts, message_text in message_history:
        logging.info(
            "schedule ts: %s, message ts: %s, streak: %d",
            schedule_ts,
            message_ts,
            current_streak,
        )
        if schedule_ts > message_ts:
            continue
        if schedule_ts + timedelta(hours=window) > message_ts:
            current_streak_start = current_streak_start or message_ts
            current_streak += 1
            schedule_ts = next(ci)
        else:
            if current_streak > longest_streak:
                longest_streak = current_streak
                longest_streak_start = current_streak_start
            current_streak_start = message_ts
            current_streak = 0
            while schedule_ts + timedelta(hours=window) < message_ts:
                schedule_ts = next(ci)

    if current_streak > longest_streak:
        longest_streak = current_streak
        longest_streak_start = current_streak_start

    streak_cap = app.config.get("max_streak")
    if streak_cap:
        streak_cap = int(streak_cap)

    def _print_streak(streak, streak_start):
        result = f"{min(streak, streak_cap) if streak_cap else streak} day(s)"
        if streak_start:
            result += f" (started on {streak_start.strftime('%Y-%m-%d')})"
        if streak_cap and streak >= streak_cap:
            result += f" **MAXED OUT!**"
        return result

    print("Current streak: " + _print_streak(current_streak, current_streak_start))
    print("Longest streak: " + _print_streak(longest_streak, longest_streak_start))
    print()
    interval_start = today - timedelta(days=6)
    days = [(interval_start + timedelta(days=i)).replace(tzinfo=None) for i in range(7)]
    last_week = {
        midnight(t).replace(tzinfo=None): m
        for t, m in message_history
        if t > interval_start
    }
    ct = croniter(schedule, start_time=interval_start, ret_type=datetime)
    scheduled = [midnight(next(ct)).replace(tzinfo=None) for _ in range(7)]

    print("Last 7 days:")
    for ts in reversed(days):
        if ts in scheduled or ts in last_week:
            print("  * %s: %s" % (ts.strftime("%a %b %d"), last_week.get(ts, "")))


@contextmanager
def redirect_stdout_stderr(stream):
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    sys.stdout = stream
    sys.stderr = stream
    try:
        yield
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr


@app.post("/")
async def slash_command(request):
    if request.form["token"][0] != request.app.phb_config["mattermost_token"]:
        raise Forbidden("Incorrect token %s!" % request.form["token"][0])

    command = request.form["text"][0] if "text" in request.form else ""
    username = request.form["user_name"][0]

    logging.info("%s says: %s", username, command)

    # workaround for using with slash commands (/phb ...)
    slash_mode = False
    if not command.startswith("@phb"):
        slash_mode = True
        command = "@phb " + command
    args = command.split()[1:]
    # workaround for freehand text
    if args[0] != "standup":
        args = shlex.split(command)[1:]

    stream = StringIO()
    with redirect_stdout_stderr(stream):
        try:
            if slash_mode:
                print("@%s says: %s" % (username, command))
            if (
                args[0] == "standup"
                and len(args) > 1
                and args[1] not in ("--username", "--date")
            ):
                context = {
                    "username": username,
                    "timezone": request.app.phb_config.get("timezone", "UTC"),
                    "text": " ".join(command[1:]),
                    "date": None,
                }
                await standup(context, request.app.conn)
            else:
                result = parser.parse_args(args)
                logging.info(result)
                context = vars(result)
                context["username"] = context.get("username") or username
                context["timezone"] = request.app.phb_config.get("timezone", "UTC")
                if result.subparser == "schedule":
                    await manage_schedule(
                        context, conn=request.app.conn, app=request.app
                    )
                if result.subparser == "121":
                    await send_question(request.app, username, hook=False)
                elif result.subparser == "schedules":
                    await get_all_schedules(context, request.app.conn)
                elif result.subparser == "standup":
                    await standup(context, request.app.conn)
                elif result.subparser == "stats":
                    await stats(context, request.app.conn)
                elif result.subparser == "help":
                    parser.print_help()
                else:
                    raise ValueError("Unknown command, see @phb help!")
        except SystemExit:
            logging.exception("Parse failure")
        except Exception as e:
            logging.exception("Error")
            return response.json(
                {"response_type": "in_channel", "text": "**ERROR**: %s" % e}
            )

    return response.json({"response_type": "in_channel", "text": stream.getvalue()})


@app.listener("before_server_start")
async def setup_db(app, loop):
    db_path = app.phb_config["database"]
    logging.info("Setting up the database at %s", db_path)
    conn = await aiosqlite.connect(
        db_path,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        isolation_level=None,
    )
    await create_schema(conn)
    app.conn = conn


async def request_standup(username, delay, app):
    await asyncio.sleep(delay)
    logging.info("Running standup request job for %s", username)
    row = await get_schedule(username, conn=app.conn)
    if not row:
        return

    schedule, window = row
    hook_url = app.phb_config["mattermost_webhook"]
    logging.info("POSTing to %s", hook_url)
    async with aiohttp.ClientSession() as session:
        async with session.post(
            hook_url,
            json={
                "text": "@%s: SITREP please (@phb standup [...], you have %d hour(s))"
                % (username, window)
            },
        ) as result:
            result.raise_for_status()

    logging.info("Sent standup request to %s", username)
    req, rem = app.reminder_state[username]
    if not rem:
        now = datetime.now(pytz.timezone(app.phb_config.get("timezone", "UTC")))
        regenerate_reminder_for_user(schedule, username, window, now, app)
    else:
        app.reminder_state[username] = None, rem


async def remind_standup(username, delay, app):
    await asyncio.sleep(delay)
    logging.info("Running standup reminder job for %s", username)
    row = await get_schedule(username, conn=app.conn)
    if not row:
        return

    schedule, window = row
    now = datetime.now(pytz.timezone(app.phb_config.get("timezone", "UTC")))
    row = await get_standup_message(username, midnight(now), app.conn)
    if not row:
        hook_url = app.phb_config["mattermost_webhook"]
        async with aiohttp.ClientSession() as session:
            async with session.post(
                hook_url,
                json={
                    "text": "@%s: You only have 1 hour left for your standup message to be counted!"
                    % username
                },
            ) as result:
                result.raise_for_status()
    regenerate_reminder_for_user(schedule, username, window, now, app)


async def regenerate_reminders(app):
    logging.info("Regenerating all reminder jobs")
    for req, rem in app.reminder_state.values():
        if req:
            req.cancel()
        if rem:
            rem.cancel()
    app.reminder_state = {}

    async with app.conn.execute(
        "SELECT username, schedule, window FROM schedule",
    ) as cur:
        schedules = await cur.fetchall()
    now = datetime.now(pytz.timezone(app.phb_config.get("timezone", "UTC")))
    for username, schedule, window in schedules:
        regenerate_reminder_for_user(schedule, username, window, now, app)


def regenerate_reminder_for_user(schedule, username, window, now, app):
    next_time = croniter(schedule, start_time=now).get_next(ret_type=datetime)
    # use astimezone().timestamp() to convert to local timestamp
    delay = (next_time - now).total_seconds()
    request_job = asyncio.create_task(request_standup(username, delay, app))
    logging.info(
        "Set up standup request job for user %s at %s (in %d)",
        username,
        next_time,
        delay,
    )
    if window > 1:
        reminder_ts = next_time + timedelta(hours=window - 1)
        delay = (reminder_ts - now).total_seconds()
        reminder_job = asyncio.create_task(remind_standup(username, delay, app))
        logging.info(
            "Set up standup reminder job for user %s at %s (in %d)",
            username,
            reminder_ts,
            delay,
        )
    else:
        reminder_job = None
    app.reminder_state[username] = (request_job, reminder_job)


@app.listener("after_server_start")
async def setup_reminders(app, loop):
    app.reminder_state = {}
    await regenerate_reminders(app)


async def send_question(app, victim=None, hook=True):
    if not victim:
        async with app.conn.execute(
            "SELECT DISTINCT(username) FROM schedule",
        ) as cur:
            victims = await cur.fetchall()
            victim = random.choice([v[0] for v in victims])
    question = random.choice([q["question"] for q in app.questions])
    logging.info("Victim %s, question %s", victim, question)

    text = "%s, %s" % (victim, question)
    logging.info(text)

    if hook:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url=app.phb_config["mattermost_webhook"],
                json={"text": text},
            ) as result:
                result.raise_for_status()
    else:
        print(text)


async def spam_coro(app):
    while True:
        delay = random.expovariate(1.0 / app.phb_config["questions_mean"])
        logging.info("Next spam: %d s (%.2f h)", delay * 3600, delay)
        await asyncio.sleep(delay * 3600)
        await send_question(app)


@app.listener("after_server_start")
async def setup_spam(app, loop):
    if "questions" in app.phb_config:
        with open(app.phb_config["questions"]) as f:
            app.questions = json.load(f)

        asyncio.create_task(spam_coro(app))


@app.listener("before_server_stop")
async def shutdown_db(app, loop):
    for req, rem in app.reminder_state.values():
        if req:
            req.cancel()
        if rem:
            rem.cancel()
    await app.conn.commit()
    await app.conn.close()


@click.command("phb")
@click.argument("config_file", default="config.yml", type=click.File("r"))
def main(config_file):
    config = yaml.load(config_file)
    app.phb_config = config
    app.run(host=config.get("host", "127.0.0.1"), port=config["port"], debug=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    main()
