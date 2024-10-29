class InputOutputException(Exception):

    def __init__(self, error_message, status_code):
        self.error_dict = {'error_message': error_message,
                           'status_code': status_code}

    def to_dict(self):
        return self.error_dict


class ValidationException(Exception):
    def __init__(self, error_message, status_code):
        self.error_dict = {'error_message': error_message,
                           'status_code': status_code}

    def to_dict(self):
        return self.error_dict


class OSRMException(Exception):
    def __init__(self, error_message, status_code):
        self.error_dict = {'error_message': error_message,
                           'status_code': status_code}

    def to_dict(self):
        return self.error_dict
