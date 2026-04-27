module.exports = {
  apps: [{
    name: "wa-server",
    script: "index.js",
    instances: 1,
    autorestart: true,
    watch: false,
    max_restarts: 20,
    restart_delay: 3000,
    env: {
      NODE_ENV: "production",
      PORT: 8080,
    },
  }],
};
