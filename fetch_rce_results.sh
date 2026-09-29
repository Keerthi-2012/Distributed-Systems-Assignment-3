#!/bin/bash
# fetch_rce_results.sh - copy the benchmark results back from the RCE cluster.
#
#   bash fetch_rce_results.sh            fetch Q1 and Q2 results, then re-plot
#   bash fetch_rce_results.sh q1         fetch only Q1
#   bash fetch_rce_results.sh q2         fetch only Q2
#   bash fetch_rce_results.sh status     just show the job queue and the logs
#
# Your RCE username and password are read from .env, which is gitignored:
#
#   RCE_USER=cs3401.26
#   RCE_PASS=...
#
# The password is fed to ssh through SSH_ASKPASS, so nothing is ever typed and
# nothing is written to your shell history. You must be on the IIIT VPN.

set -u
cd "$(dirname "$0")"
ROOT=$(pwd)
ENV_FILE=$ROOT/.env
WHAT=${1:-all}

[ -f "$ENV_FILE" ] || { echo "no .env - create it with RCE_USER= and RCE_PASS="; exit 1; }

read_env() {   # read_env KEY - prints the value, quotes stripped
    python3 -c "
import sys
for line in open('$ENV_FILE'):
    if line.startswith('$1='):
        print(line.split('=', 1)[1].strip().strip('\"').strip(\"'\"))
        break
"
}

USER_NAME=$(read_env RCE_USER)
HOST=${RCE_HOST:-rce.iiit.ac.in}
[ -n "$USER_NAME" ] || { echo "RCE_USER missing from .env"; exit 1; }

# Where Section2 lives on the cluster. NOT "~/HW3/Section2": the shell would
# expand ~ to YOUR home here, and rsync would then ask the cluster for
# /home/meet-ghelani/... which does not exist there. A relative path is resolved
# against the remote home directory, which is what we want.
REMOTE=HW3/Section2

# ---- feed the password to ssh without a terminal ----------------------------
ASKPASS=$(mktemp)
cat > "$ASKPASS" <<ASK
#!/bin/bash
python3 -c "
for line in open('$ENV_FILE'):
    if line.startswith('RCE_PASS='):
        print(line.split('=', 1)[1].strip().strip('\"').strip(\"'\"))
        break
"
ASK
chmod 700 "$ASKPASS"
trap 'rm -f "$ASKPASS"' EXIT

SSH_OPTS="-o StrictHostKeyChecking=accept-new -o ConnectTimeout=30
          -o NumberOfPasswordPrompts=1 -o PreferredAuthentications=password
          -o PubkeyAuthentication=no"

rsh() {   # rsh <command...> - run a command on the login node
    SSH_ASKPASS="$ASKPASS" SSH_ASKPASS_REQUIRE=force DISPLAY=:0 \
    setsid -w ssh $SSH_OPTS "$USER_NAME@$HOST" "$@" 2>&1 \
        | grep -v "post-quantum\|store now\|openssh.com/pq\|may need to be upgraded"
}

pull() {   # pull <question folder>
    local q=$1
    echo "== $q =="
    mkdir -p "$ROOT/Section2/$q/results"
    RSYNC_RSH="env SSH_ASKPASS=$ASKPASS SSH_ASKPASS_REQUIRE=force DISPLAY=:0 setsid -w ssh $SSH_OPTS" \
    rsync -az --info=name \
        --exclude '*_[0-9]*.log' \
        "$USER_NAME@$HOST:$REMOTE/$q/results/" "$ROOT/Section2/$q/results/" \
        2>&1 | grep -v "post-quantum\|store now\|openssh.com/pq\|may need to be upgraded"
}

# ---- status only ------------------------------------------------------------
if [ "$WHAT" = "status" ]; then
    echo "== jobs =="
    rsh 'squeue -u $USER -o "%i %j %T %M %D %R"'
    echo ""
    echo "== most recent benchmark logs =="
    rsh "ls -t $REMOTE/Q1_mapreduce/results/*.log $REMOTE/Q2_grpc/results/*.log 2>/dev/null | head -4"
    echo ""
    echo "== tail of the newest Q1 log =="
    rsh "tail -15 \$(ls -t $REMOTE/Q1_mapreduce/results/q1_bench_*.log 2>/dev/null | head -1)"
    exit 0
fi

# ---- fetch ------------------------------------------------------------------
if [ "$WHAT" = "all" ] || [ "$WHAT" = "q1" ]; then
    pull Q1_mapreduce
fi
if [ "$WHAT" = "all" ] || [ "$WHAT" = "q2" ]; then
    pull Q2_grpc
fi

# ---- re-draw the figures from whatever came back ----------------------------
# The plots need matplotlib. Prefer the project's virtualenv if there is one,
# since the system python3 usually does not have it.
PLOT_PY=python3
[ -x "$ROOT/.venv/bin/python3" ] && PLOT_PY=$ROOT/.venv/bin/python3

echo ""
if ! "$PLOT_PY" -c "import matplotlib" 2>/dev/null; then
    echo "== plots skipped: matplotlib is not installed =="
    echo "   install it with:  $PLOT_PY -m pip install matplotlib"
    echo "   then re-draw with:"
    echo "     (cd Section2/Q1_mapreduce && $PLOT_PY scripts/plot_q1.py)"
    echo "     (cd Section2/Q2_grpc     && $PLOT_PY scripts/plot_q2.py)"
else
    echo "== plots =="
    if [ "$WHAT" = "all" ] || [ "$WHAT" = "q1" ]; then
        (cd "$ROOT/Section2/Q1_mapreduce" && "$PLOT_PY" scripts/plot_q1.py)
    fi
    if [ "$WHAT" = "all" ] || [ "$WHAT" = "q2" ]; then
        (cd "$ROOT/Section2/Q2_grpc" && "$PLOT_PY" scripts/plot_q2.py)
    fi
fi

echo ""
echo "done. Rebuild the report with:"
echo "    cd report_build && python3 gen.py && pdflatex -interaction=nonstopmode report.tex"
