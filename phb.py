import argparse
import logging
import shlex
import sqlite3
import sys
from contextlib import contextmanager
from datetime import datetime, timedelta
from io import StringIO

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


async def manage_schedule(context, conn):
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


async def get_standup_message(username, date, conn):
    async with conn.execute(
        "SELECT message, timestamp FROM messages "
        "WHERE username = ? AND date(timestamp) = date(?)",
        parameters=(username, date),
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
        update_date = midnight(tz.localize(context["date"]))
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

    reply = "@%s Thanks!" % username
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

    print(
        "Current streak: %d day(s)" % current_streak
        + (
            (" (started on %s)" % current_streak_start.strftime("%Y-%m-%d"))
            if current_streak_start
            else ""
        )
    )
    print(
        "Longest streak: %d day(s)" % longest_streak
        + (
            (" (started on %s)" % longest_streak_start.strftime("%Y-%m-%d"))
            if longest_streak_start
            else ""
        )
    )

    print()
    interval_start = today - timedelta(days=6)
    days = [interval_start + timedelta(days=i) for i in range(7)]
    last_week = {midnight(t): m for t, m in message_history if t > interval_start}
    ct = croniter(schedule, start_time=interval_start, ret_type=datetime)
    scheduled = [midnight(next(ct)) for _ in range(7)]

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

    args = command.split()[1:]
    # workaround for freehand text
    if args[0] != "standup":
        args = shlex.split(command)[1:]

    stream = StringIO()
    with redirect_stdout_stderr(stream):
        try:
            result = parser.parse_args(args)
            logging.info(result)
            context = vars(result)
            context["username"] = context.get("username") or username
            context["timezone"] = request.app.phb_config.get("timezone", "UTC")
            if result.subparser == "schedule":
                await manage_schedule(context, conn=request.app.conn)
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


@app.listener("before_server_stop")
async def shutdown_db(app, loop):
    await app.conn.commit()
    await app.conn.close()


@click.command("phb")
@click.argument("config_file", default="config.yml", type=click.File("r"))
def main(config_file):
    config = yaml.load(config_file)
    app.phb_config = config
    app.run(host="127.0.0.1", port=config["port"], debug=True)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    main()
