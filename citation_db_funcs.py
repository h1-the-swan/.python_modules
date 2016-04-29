def parse_id(id_to_parse=None):
    # Should take ID in any form (str, int, list of int/str, pandas series) and return a list of IDs as strings
    if isinstance(id_to_parse, basestring):
        idlist = [id_to_parse]
        return idlist
    else:
        try:
            idlist = list(id_to_parse)
            idlist = [str(item) for item in idlist]
            return idlist
        except TypeError:
            idlist = [str(int(id_to_parse))]
            return idlist
    return id_to_parse

