from multiprocessing import get_context

# always use spawn as it's supported on all major OS
# see https://docs.python.org/3/library/multiprocessing.html#contexts-and-start-methods
multiprocessing = get_context("spawn")
