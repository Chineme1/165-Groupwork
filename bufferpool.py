
class BufferPool:

    def __init__(self, num_pages):
        self.size = num_pages   # size of the bufferpool
        self.bufferpool = []


    """
    # Evit a page when the bufferpool is full
    """
    def evit(self):
        # find the ear
        pass

    """
    # Flush the dirty pages back to disk
    """
    def flush(self):
        # call upon replacement or when database is closed
        pass

    """
    # pin or unpin a page
    """
    def pin_or_unpin(self):
        pass
