import re


class FileNameError(Exception):
    """
    exception to handle when filename is not of correct pattern
    """
    def __init__(self, filename):
        Exception.__init__(
            self,
            'Filename \'{}\' was not correct pattern'.format(filename)
        )
        self.filename = filename


class FileNameMatch(object):
    """
    Matches eggs or jars for both released and snapshot versions

    Supported Patterns:
      new_library-1.0.0-py3.6.egg
      new_library-1.0.0-SNAPSHOT-py3.6.egg
      new_library-1.0.0-SNAPSHOT-my-branch-py3.6.egg

      new_library-1.0.0.egg
      new_library-1.0.0-SNAPSHOT.egg
      new_library-1.0.0-SNAPSHOT-my-branch.egg

      new_library-1.0.0.jar
      new_library-1.0.0-SNAPSHOT.jar
      new_library-1.0.0-SNAPSHOT-my-branch.jar

    Parameters
    ----------
    library_name: string
        base name of library (e.g. 'test_library')
    version: string
        version of library (e.g. '1.0.0')

    """
    file_pattern = (
        r'([a-zA-Z0-9-\._]+)-((\d+)\.(\d+\.\d+)'
        r'(?:-SNAPSHOT(?:[a-zA-Z_\-\.]+)?)?)(?:-py.+)?\.(egg|jar)'
    )

    def __init__(self, filename):
        match = re.match(FileNameMatch.file_pattern, filename)
        try:
            self.filename = filename
            self.library_name = match.group(1)
            self.version = match.group(2)
            self.major_version = match.group(3)
            self.minor_version = match.group(4)
            self.suffix = match.group(5)
            if self.suffix == 'jar':
                self.lib_type = 'java-jar'
            elif self.suffix == 'egg':
                self.lib_type = 'python-egg'
        except (IndexError, AttributeError):
            raise FileNameError(filename)

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            self_attrs = {k: v for k, v in vars(self).items()}
            self_attrs.pop('filename')
            other_attrs = {k: v for k, v in vars(other).items()}
            other_attrs.pop('filename')
            return self_attrs == other_attrs
        else:
            return False

    def replace_version(self, other, logger):
        """
        True if self can safely replace other

        based on version numbers only - snapshot and branch tags are ignored
        """

        if other.library_name != self.library_name:
            logger.debug(
                'not replacable: {} != {} ()'
                .format(other.library_name, self.library_name)
            )
            return False
        elif int(other.major_version) != int(self.major_version):
            logger.debug(
                'not replacable: {} != {} ({})'
                .format(
                    int(self.major_version),
                    int(other.major_version),
                    other.filename,
                )
            )
            return False
        elif float(other.minor_version) >= float(self.minor_version):
            logger.debug(
                'not replacable: {} >= {} ({})'
                .format(
                    other.minor_version,
                    self.minor_version,
                    other.filename,
                )
            )
            return False
        else:
            return True
