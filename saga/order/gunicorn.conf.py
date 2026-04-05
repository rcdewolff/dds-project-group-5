def on_starting(server):
    import asyncio
    from app import init_db
    asyncio.run(init_db())
