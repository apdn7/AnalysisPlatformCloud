# MAX_MESSAGE_LENGTH = 100 * 1024 * 1024
from flask import Flask

MAX_MESSAGE_LENGTH = -1


def serve_grpc_server(app: Flask, port: int):
    from concurrent import futures

    import grpc

    from ap import init_db
    from ap.common.logger import logger
    from grpc_server.connection_controller import ConnectionController
    from grpc_src.connection.services_pb2_grpc import add_ConnectionServicer_to_server

    # gen database
    init_db(app)

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', MAX_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_MESSAGE_LENGTH),
        ],
    )
    host = '0.0.0.0'
    port = port or 8000
    # =======
    add_ConnectionServicer_to_server(ConnectionController(), server)
    # add_TraceGraphServicer_to_server(TracingGraphController(), server)
    # add_SettingServicer_to_server(SettingController(), server)
    # =======
    server.add_insecure_port(f'{host}:{port}')
    server.start()
    logger.info(f'server started:{host}:{port}')
    server.wait_for_termination()

    # ServerInfo.initialize()

    # init_scheduler()

    # print(f'Bridge Station\'s disk usage: {get_disk_usage_percent()[1]}%')

    # handle_duplicate_data_job()
