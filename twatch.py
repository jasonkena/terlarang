#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#     "psutil",
#     "requests",
#     "iterfzf",
# ]
# ///

import os
import argparse
import subprocess
import time
import datetime
import iterfzf
import psutil
import requests


def run_command(args):
    # https://stackoverflow.com/questions/4760215/running-shell-command-and-capturing-the-output
    return subprocess.run(args, stdout=subprocess.PIPE).stdout.decode("utf-8")


def get_pane_processes():
    output = run_command(["tmux", "list-panes", "-a", "-F", "#{pane_pid} #{pane_id}"])
    output = output.split("\n")
    output = [x for x in output if x]

    # {PID: pane#}
    pids = []
    panes = []
    for line in output:
        pid, pane = line.split(" ")
        assert pane[0] == "%"
        pids.append(pid)
        panes.append(pane)

    return pids, panes


def get_forest(pid):
    output = run_command(["ps", "-o", "pid,command", "--forest", "-g", pid])
    print(output)
    output = output.split("\n")
    output = [x.strip() for x in output if x]
    output = [x for x in output if x]
    assert output[0] == "PID COMMAND"
    output = output[1:]

    pids = [x.split()[0] for x in output]

    return pids, output


def get_pane_output(pane):
    """Get current output from a tmux pane"""
    output = run_command(["tmux", "capture-pane", "-t", pane, "-p", "-J", "-S-"])
    return output.strip()


def discord(message):
    """Send a message to Discord"""
    discord_webhook = os.environ["DISCORD_WEBHOOK"]
    discord_uid = os.environ["DISCORD_UID"]
    requests.post(
        discord_webhook,
        json={"content": f"<@{discord_uid}> {message}"},
    )


def send_timeout_notification(selected, pane, timeout_secs):
    """Send discord notification when stdout hasn't updated"""
    discord(
        f"process {selected} in pane {pane} has no stdout updates for {timeout_secs} seconds"
    )


def on_exit(selected, slurm_job_id, kill, kill_wait_for, save_to):
    _, panes = get_pane_processes()
    output_dir = os.path.join(
        save_to, datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    )
    # Save terminal output
    os.makedirs(output_dir, exist_ok=True)
    for pane in panes:
        output = get_pane_output(pane)
        with open(os.path.join(output_dir, pane), "w") as f:
            f.write(output)

    print(f"output saved to {output_dir}")

    if kill:
        discord(
            f"process {selected} has exited, output saved to {output_dir}, killing slurm job {slurm_job_id} in {kill_wait_for} seconds"
        )
    else:
        discord(f"process {selected} has exited, output saved to {output_dir}")

    # kill slurm
    if kill:
        time.sleep(kill_wait_for)
        run_command(["scancel", slurm_job_id])


def main(kill, wait_output=None, check_every=10, kill_wait_for=600, save_to=None):
    slurm_job_id = os.environ.get("SLURM_JOB_ID")

    prompts = []
    pids, panes = get_pane_processes()
    for i in range(len(pids)):
        child_pids, output = get_forest(pids[i])
        for j in range(len(child_pids)):
            prompts.append((child_pids[j], panes[i], output[j]))

    output_to_stuff = {x[2]: (x[0], x[1]) for x in prompts}
    selected = iterfzf.iterfzf([x[-1] for x in prompts][::-1])
    if selected is None:
        print("no process selected, exiting")
        return

    child_pid, pane = output_to_stuff[selected]

    print(f"SLURM_JOB_ID={slurm_job_id}")
    print(f"monitoring {child_pid} in pane {pane}")

    if wait_output:
        last_output = get_pane_output(pane)
        last_output_time = time.time()
        print(f"also monitoring stdout with {wait_output}s timeout")

    while True:
        if not psutil.pid_exists(int(child_pid)):
            if kill:
                print(
                    f"process {child_pid} in pane {pane} has exited, kill to stop trigger"
                )
            else:
                print(f"process {child_pid} in pane {pane} has exited")
            on_exit(selected, slurm_job_id, kill, kill_wait_for, save_to)
            break

        # Check for stdout updates if monitoring is enabled
        if wait_output:
            current_output = get_pane_output(pane)
            current_time = time.time()

            if current_output != last_output:
                # Output has changed, reset timer
                last_output = current_output
                last_output_time = current_time
            elif current_time - last_output_time >= wait_output:
                # No output update within timeout period
                send_timeout_notification(selected, pane, wait_output)
                wait_output = None  # Disable after first trigger

        time.sleep(check_every)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kill", action="store_true")
    # note that this only triggers ONCE
    parser.add_argument(
        "--wait-output",
        type=int,
        default=300,
        help="Monitor stdout and notify if no updates for this many seconds",
    )
    parser.add_argument(
        "--check-every",
        type=int,
        default=10,
        help="Check interval in seconds (default: 10)",
    )
    parser.add_argument(
        "--kill-wait-for",
        type=int,
        default=600,
        help="Wait time before killing SLURM job in seconds (default: 600)",
    )
    parser.add_argument(
        "--save-to",
        type=str,
        default="/home/adhinart/twatch",
        help="Directory to save terminal outputs (default: /home/adhinart/twatch)",
    )

    args = parser.parse_args()

    main(
        kill=args.kill,
        wait_output=args.wait_output,
        check_every=args.check_every,
        kill_wait_for=args.kill_wait_for,
        save_to=args.save_to,
    )
