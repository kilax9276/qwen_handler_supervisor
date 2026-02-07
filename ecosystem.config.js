module.exports = {
  apps: [
    {
      name: "orchestrator",
      cwd: "/home/docker/QwenHandlerSupervisorMulticontainer",   
      script: ".venv/bin/uvicorn",
      args: "src.app.main:app --host 0.0.0.0 --port 9000 --log-level debug --access-log",
      interpreter: "none",
      time: true,

      merge_logs: true,

      env: {

        CONFIG_PATH: "/home/docker/QwenHandlerSupervisorMulticontainer/config.yaml",
        SQLITE_PATH: "/home/docker/QwenHandlerSupervisorMulticontainer/data/mvp.sqlite",

        PYTHONUNBUFFERED: "1",

        SERVER_HOST: "0.0.0.0",
        SERVER_PORT: "9000"
      }
    }
  ]
};
