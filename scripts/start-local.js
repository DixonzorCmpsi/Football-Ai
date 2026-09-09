const { spawn } = require('child_process');
const path = require('path');

const mode = process.argv[2] || 'local';
const root = path.resolve(__dirname, '..');
const isWindows = process.platform === 'win32';

const command = isWindows ? 'cmd.exe' : 'bash';
const args = isWindows
  ? ['/c', path.join(root, 'start.bat'), mode]
  : [path.join(root, 'start.sh'), mode];

const child = spawn(command, args, {
  cwd: root,
  stdio: 'inherit',
  shell: false,
});

child.on('exit', (code, signal) => {
  if (signal) {
    process.kill(process.pid, signal);
    return;
  }
  process.exit(code ?? 0);
});
