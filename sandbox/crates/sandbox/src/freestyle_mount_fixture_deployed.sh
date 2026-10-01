set -e
mkdir -p /run/chevalier/mounts
if [ ! -e '/run/chevalier/mounts/root' ]; then
  mkdir -p '/nym'
  export CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN='synthetic'
  export CHEVALIER_VFS_OWNER='nym'
  export CHEVALIER_VFS_READ_ONLY=true
  nohup '/usr/local/bin/chevalier-vfs-fuse' '--endpoint' 'http://127.0.0.1:18991' '--scope' 'root' '--tag' 'root' '--state-dir' '/var/lib/lane-b/root' '/nym' >'/run/chevalier/mounts/root.log' 2>&1 </dev/null &
  echo $! > '/run/chevalier/mounts/root'
fi
pid=$(cat '/run/chevalier/mounts/root' 2>/dev/null || echo 0)
for _ in $(seq 1 560); do mountpoint -q '/nym' && break; kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
if [ ! -e '/run/chevalier/mounts/shared' ]; then
  mkdir -p '/nym/vm/mounts/shared'
  export CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN='synthetic'
  export CHEVALIER_VFS_OWNER='nym'
  export CHEVALIER_VFS_READ_ONLY=false
  nohup '/usr/local/bin/chevalier-vfs-fuse' '--endpoint' 'http://127.0.0.1:18991' '--scope' 'shared' '--tag' 'shared' '--state-dir' '/var/lib/lane-b/shared' '/nym/vm/mounts/shared' >'/run/chevalier/mounts/shared.log' 2>&1 </dev/null &
  echo $! > '/run/chevalier/mounts/shared'
fi
pid=$(cat '/run/chevalier/mounts/shared' 2>/dev/null || echo 0)
for _ in $(seq 1 560); do mountpoint -q '/nym/vm/mounts/shared' && break; kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
if [ ! -e '/run/chevalier/mounts/task' ]; then
  mkdir -p '/nym/vm/mounts/task'
  export CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN='synthetic'
  export CHEVALIER_VFS_OWNER='nym'
  export CHEVALIER_VFS_READ_ONLY=false
  nohup '/usr/local/bin/chevalier-vfs-fuse' '--endpoint' 'http://127.0.0.1:18991' '--scope' 'task' '--tag' 'task' '--state-dir' '/var/lib/lane-b/task' '/nym/vm/mounts/task' >'/run/chevalier/mounts/task.log' 2>&1 </dev/null &
  echo $! > '/run/chevalier/mounts/task'
fi
pid=$(cat '/run/chevalier/mounts/task' 2>/dev/null || echo 0)
for _ in $(seq 1 560); do mountpoint -q '/nym/vm/mounts/task' && break; kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
if [ ! -e '/run/chevalier/mounts/skills' ]; then
  mkdir -p '/nym/vm/mounts/skills'
  export CHEVALIER_SANDBOX_VFS_INTERNAL_SERVICE_TOKEN='synthetic'
  export CHEVALIER_VFS_OWNER='nym'
  export CHEVALIER_VFS_READ_ONLY=true
  nohup '/usr/local/bin/chevalier-vfs-fuse' '--endpoint' 'http://127.0.0.1:18991' '--scope' 'skills' '--tag' 'skills' '--state-dir' '/var/lib/lane-b/skills' '/nym/vm/mounts/skills' >'/run/chevalier/mounts/skills.log' 2>&1 </dev/null &
  echo $! > '/run/chevalier/mounts/skills'
fi
pid=$(cat '/run/chevalier/mounts/skills' 2>/dev/null || echo 0)
for _ in $(seq 1 560); do mountpoint -q '/nym/vm/mounts/skills' && break; kill -0 "$pid" 2>/dev/null || break; sleep 0.5; done
