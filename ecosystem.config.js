module.exports = {
  apps: [
    {
      name: 'profile-crawler',
      script: 'uv',
      args: 'run python workers/profile_crawler.py --max 100 --delay 1.0 --retries 3',
      cwd: '/Users/rob/repos/artifish',
      instances: 1,
      autorestart: false,  // Don't auto-restart when it finishes
      watch: false,
      max_memory_restart: '1G',
      cron_restart: '*/30 * * * *',  // Restart every 30 minutes
      env: {
        NODE_ENV: 'development'
      },
      env_production: {
        NODE_ENV: 'production'
      },
      log_file: './logs/profile-crawler.log',
      out_file: './logs/profile-crawler-out.log',
      error_file: './logs/profile-crawler-error.log',
      time: true
    },
    {
      name: 'sentiment-analyzer',
      script: 'uv',
      args: 'run python workers/sentiment_analyzer.py',
      cwd: '/Users/rob/repos/artifish',
      instances: 1,
      autorestart: true,
      watch: false,
      max_memory_restart: '500M',
      log_file: './logs/sentiment-analyzer.log',
      out_file: './logs/sentiment-analyzer-out.log',
      error_file: './logs/sentiment-analyzer-error.log',
      time: true
    }
  ]
};