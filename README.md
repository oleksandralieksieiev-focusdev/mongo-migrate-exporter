# mongo-migrate-exporter


## Build:
  npm install
  npm run build

## Run:
### Export mode (default)
  ./dist/bundle.js --uri mongodb://localhost:27017 --db mydb --out ./backups --gzip

### Import mode
  ./dist/bundle.js --mode import --uri mongodb://doesntmatter:1 --db dummy --dest-uri mongodb://localhost:27017 --dest-db otherdb --out ./backups

Note: supply environment variables via .env or OS env variables. See .env.example
