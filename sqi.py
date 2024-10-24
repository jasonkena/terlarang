#!/usr/bin/env python3
import re
import sys
import subprocess
from datetime import datetime


def generate_slurm_script(partition):
    is_gpu = any([partition in p for p in ["gpua100", "gpuv100", "weidf"]])
    mem = {"gpua100": "240GB", "gpuv100": "185GB", "weidf": "495GB"}
    mem = mem[partition] if is_gpu else "185GB"

    hours = max_hours_for_job(partition)
    print(f"Max hours for job: {hours}")

    script = """#!/usr/bin/env bash
#SBATCH -J gpu_task
#SBATCH --output=main_%j.out
#SBATCH -p {}
#SBATCH --mem={}
#SBATCH -n 1
#SBATCH --cpus-per-task=48
#SBATCH -t {}:00:00
#SBATCH --error=/data/adhinart/tmp/main_%j.err
#SBATCH --output=/data/adhinart/tmp/main_%j.out
#SBATCH --exclusive
{}
grep MemFree /proc/meminfo | awk '{{print $2, "/ 1000000"}}' | bc

# https://unix.stackexchange.com/questions/659150/tmux-sessions-get-killed-on-ssh-logout
export XDG_RUNTIME_DIR=/run/user/$(id -u)
loginctl enable-linger adhinart
#tmux new-session -d
systemd-run --scope --user tmux new-session -d

sleep infinity""".format(
        partition, mem, hours, "#SBATCH --gpus-per-node=4" if is_gpu else ""
    )

    return script


def max_hours_for_job(partition):
    scontrol_output = run_command(["scontrol", "show", "reservation"])
    reservations = parse_reservations(scontrol_output)
    earliest_reservations = nodes_earliest_reservation(reservations)

    sinfo_output = run_command(["sinfo"])
    partition_dict = parse_sinfo(sinfo_output)

    furthest_time = furthest_reservation_time(
        partition, partition_dict, earliest_reservations
    )
    return hours_to_furthest_time(furthest_time)


def run_command(args):
    # https://stackoverflow.com/questions/4760215/running-shell-command-and-capturing-the-output
    return subprocess.run(args, stdout=subprocess.PIPE).stdout.decode("utf-8")


def expand_nodes(s):
    # Split by commas not within brackets
    parts = re.split(r",(?![^\[]*\])", s)
    result = []

    for part in parts:
        match = re.match(r"([a-z]+)\[(.+)\]", part)
        if match:
            base = match.group(1)
            ranges = match.group(2).split(",")
            for r in ranges:
                if "-" in r:
                    start, end = map(int, r.split("-"))
                    for i in range(start, end + 1):
                        result.append(f"{base}{i:03}")
                else:
                    result.append(f"{base}{int(r):03}")
        else:
            result.append(part)

    return result


def parse_reservations(scontrol_output):
    if "No reservations in the system" in scontrol_output:
        return []
    reservations = []
    reservation_blocks = scontrol_output.strip().split("\n\n")

    for block in reservation_blocks:
        reservation = {}
        lines = block.split("\n")

        reservation_name_match = re.search(r"ReservationName=(\S+)", lines[0])
        if reservation_name_match:
            reservation["ReservationName"] = reservation_name_match.group(1)

        start_time_match = re.search(r"StartTime=(\S+)", lines[0])
        if start_time_match:
            reservation["StartTime"] = datetime.strptime(
                start_time_match.group(1), "%Y-%m-%dT%H:%M:%S"
            )

        nodes_match = re.search(r"Nodes=(\S+)", lines[1])
        if nodes_match:
            node_range = nodes_match.group(1)
            reservation["Nodes"] = expand_nodes(node_range)

        reservations.append(reservation)

    return reservations


def nodes_earliest_reservation(reservations):
    node_times = {}
    for res in reservations:
        for node in res["Nodes"]:
            if node not in node_times or res["StartTime"] < node_times[node]:
                node_times[node] = res["StartTime"]
    return node_times


def parse_sinfo(partition_output):
    partition_dict = {}
    lines = partition_output.strip().split("\n")[1:]  # Skip header line

    for line in lines:
        parts = line.split()
        partition_name = parts[0]
        nodelist = parts[-1] if parts[-2] != "n/a" else ""

        if nodelist and nodelist != "n/a":
            nodes = expand_nodes(nodelist)
        else:
            nodes = []

        if partition_name not in partition_dict:
            partition_dict[partition_name] = []

        partition_dict[partition_name].extend(nodes)

    return partition_dict


def furthest_reservation_time(partition, partition_dict, earliest_reservations):
    nodes = partition_dict.get(partition, [])
    furthest_time = None

    for node in nodes:
        if node in earliest_reservations:
            if furthest_time is None or earliest_reservations[node] > furthest_time:
                furthest_time = earliest_reservations[node]
        else:
            return None

    return furthest_time


def hours_to_furthest_time(furthest_time):
    if furthest_time is None:
        return 120

    current_time = datetime.now()
    time_difference = furthest_time - current_time
    hours_difference = (
        time_difference.total_seconds() // 3600
    )  # Convert seconds to hours and round down

    return min(int(hours_difference), 120)


if __name__ == "__main__":
    # Check if the correct number of arguments is provided
    if len(sys.argv) != 2:
        print("Usage: python generate_and_submit_slurm_script.py <partition>")
        sys.exit(1)

    partition = sys.argv[1]
    slurm_script = generate_slurm_script(partition)

    # Write the SLURM script to a file
    path = "/tmp/my_slurm_script.sl"
    with open(path, "w") as file:
        print(f"wrote to {path}")
        file.write(slurm_script)

    # Submit the SLURM script using sbatch
    try:
        subprocess.run(["sbatch", path], check=True)
        print("SLURM script submitted successfully.")
    except subprocess.CalledProcessError as e:
        print("Error occurred while submitting the SLURM script.")
        print(e)
