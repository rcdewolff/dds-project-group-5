
# Start only one instance of db for both workerss
def on_starting(server):
    from app import init_db
    init_db()

# Introduce kafka lazy setup, it waits for gunicorn to be up and then starts the clients
def post_fork(server, worker):
    import app  
    app.db_pool = app.init_db_pool()
