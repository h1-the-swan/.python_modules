# http://stackoverflow.com/questions/3012421/python-lazy-property-decorator
# or
# https://www.safaribooksonline.com/library/view/python-cookbook-3rd/9781449357337/ch08s10.html
# lazy property: compute something once when it is called, then cache it
class lazyproperty():
    """lazy property: compute something once when it is called, then cache it
    use as a decorator

    """

    def __init__(self, func):
        self.func = func

    def __get__(self, instance, cls):
        if instance is None:
            return self
        else:
            value = self.func(instance)
            setattr(instance, self.func.__name__, value)
            return value
