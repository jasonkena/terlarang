#!/mmfs1/data/adhinart/mambaforge/envs/chunk/bin/python
import os
import argparse
import iterfzf
import subprocess
import psutil
import time
import datetime
import requests

USER = "adhinart"
CHECK_EVERY = 10
WAIT_FOR = 600
SAVE_TO = "/data/adhinart/twatch"
DISCORD_WEBHOOK = "https://discord.com/api/webhooks/1266511565314855084/qh0JkPetfqq6B8-j9KSp-2EvPyBpmPnVzntH597YNXPDIKOAt84npa9horbBH625_bF2"
DISCORD_UID = "242219798140944384"


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


def on_exit(selected, slurm_job_id, kill):
    _, panes = get_pane_processes()
    output_dir = os.path.join(
        SAVE_TO, datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    )
    # Save terminal output
    os.makedirs(output_dir, exist_ok=True)
    for pane in panes:
        output = run_command(["tmux", "capture-pane", "-t", pane, "-p", "-J", "-S-"])
        # only remove trailing spaces
        output = "\n".join([x.rstrip() for x in output.split("\n")])

        with open(os.path.join(output_dir, pane), "w") as f:
            f.write(output)

    print(f"output saved to {output_dir}")

    requests.post(
        DISCORD_WEBHOOK,
        json={
            "content": f"<@{DISCORD_UID}> process {selected} has exited, output saved to {output_dir}, killing slurm job {slurm_job_id} in {WAIT_FOR} seconds"
            if kill
            else f"<@{DISCORD_UID}> process {selected} has exited, output saved to {output_dir}"
        },
    )

    # kill slurm
    if kill:
        time.sleep(WAIT_FOR)
        run_command(["scancel", slurm_job_id])


def main(kill):
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
    while True:
        if not psutil.pid_exists(int(child_pid)):
            if kill:
                print(
                    f"process {child_pid} in pane {pane} has exited, kill to stop trigger"
                )
            else:
                print(f"process {child_pid} in pane {pane} has exited")
            on_exit(selected, slurm_job_id, kill)
            break
        else:
            time.sleep(CHECK_EVERY)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kill", action="store_true")

    args = parser.parse_args()

    main(kill=args.kill)
