def memoize(f):
    """ Memoization decorator for functions taking one or more arguments. """
    class memodict(dict):
        def __init__(self, f):
            self.f = f
        def __call__(self, *args):
            return self[args]
        def __missing__(self, key):
            ret = self[key] = self.f(*key)
            return ret
    return memodict(f)


def memoize_1(f):
    """ Memoization decorator for functions taking one or more arguments but memoized on the first. """
    class memodict(dict):
        def __init__(self, f):
            self.f = f
        def __call__(self, *args):
            print("Args: "+str(args))
            if args[0] in self:
                print("Hit! Returning "+str(self[args[0]]))
                return self[args[0]]
            else:
                print("Miss!")
                return self.__missing__(args)
        def __missing__(self, key):
            ret = self[key[0]] = self.f(*key)
            return ret
    return memodict(f)
