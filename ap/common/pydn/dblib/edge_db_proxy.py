class EdgeDbProxy:
    """
    Support auto commit/rollback for edge db. (edge_db.sqlite)
    """

    session: None

    def __init__(self):
        from ap import db

        self.session = db.session

    def __enter__(self):
        return self.session

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type:
                self.session.rollback()
            else:
                self.session.commit()

        except Exception as e:
            self.session.rollback()
            raise e
        finally:
            pass
            # do nothing ?
            # self.db_instance.disconnect()
        return False
