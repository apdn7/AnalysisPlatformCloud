def create_command(app, **kwargs):
    from .commands import cli

    app.cli.add_command(cli)
