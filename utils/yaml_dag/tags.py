SEQUENTIAL_TAG = '!sequential'
PARALLEL_TAG = '!parallel'


class TaggedGroup(tuple):
    def __new__(cls, tag, name):
        return tuple.__new__(cls, [tag, name])

    def __repr__(self):
        return "TaggedGroup(%s, %s)" % self

    def __str__(self):
        return "[%s] %s" % self


def tagged_step_constructor(loader, node):
    return TaggedGroup(node.tag, node.value)
