def strip_whitespace(string_value):
    """
    Return the input string without space, tab,
    or newline characters (for comparing strings)
    """
    return ''.join(
        [c for c in string_value if c != ' ' and c != '\n' and c != '\t']
    )


def assert_equal_ignore_whitespace(a, b):
    """
    Assert the two strings or two lists of strings are equal except for whitespace differences
    """
