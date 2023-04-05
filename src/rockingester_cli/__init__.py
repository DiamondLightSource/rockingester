from importlib.metadata import version

__version__ = version("rockingester")
del version

__all__ = ["__version__"]
