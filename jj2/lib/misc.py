import re


JJ2_FORMAT_CHARS = re.compile(r'(§(/|\d|\w)|\|)')


def unformat_jj2_string(string):
    return re.sub(JJ2_FORMAT_CHARS, '', string)
