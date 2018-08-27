def compare_multiline_strings(actual, expected):
    """
    Helper function to compare multiline strings
    that removes new line characters/empty strings
    and turns the strings into lists.
    Then the lists are asserted to be equal to one another
    """
    actual = actual.split('\n')
    actual = [item for item in actual if item != '']
    expected = expected.split('\n')
    expected = [item for item in expected if item != '']
    assert actual == expected
