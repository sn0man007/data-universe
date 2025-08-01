import logging
import sys

def setup_logging():
    """
    Configures the root logger for the entire application.

    This function establishes a centralized logging system that outputs
    structured, formatted log messages to the console. It is designed to be
    called only once when the application first starts up.
    """
    # 1. Define the standardized format for all log messages.
    #    - %(asctime)s: The timestamp when the log was created.
    #    - %(name)s: The name of the logger (usually the module name, e.g., 'target_provider').
    #    - %(levelname)s: The severity level of the message (e.g., INFO, WARNING, CRITICAL).
    #    - %(message)s: The actual log message you write.
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)

    # 2. Get the application's root logger.
    #    Configuring the root logger ensures that any logger created later
    #    in any module will automatically inherit this configuration.
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO) # Set the minimum level of messages to process.

    # 3. Add a handler to send logs to the console (standard output).
    #    The 'if not root_logger.handlers:' check is a safeguard to prevent
    #    adding duplicate handlers if this function is accidentally called more than once.
    if not root_logger.handlers:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    logging.info("Logging has been configured successfully.")
