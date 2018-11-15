import pytest

from apparate.update_databricks_library import (
    FileNameError,
    FileNameMatch,
)


def test_filename_match_egg_with_py():
    match = FileNameMatch('new_library-1.0.0-py3.6.egg')
    assert match.library_name == 'new_library'
    assert match.version == '1.0.0'
    assert match.major_version == '1'
    assert match.minor_version == '0.0'
    assert match.suffix == 'egg'
    assert match.lib_type == 'python-egg'


def test_filename_match_egg_snapshot_with_py():
    match = FileNameMatch('new_library-1.0.0-SNAPSHOT-py3.6.egg')
    assert match.library_name == 'new_library'
    assert match.version == '1.0.0-SNAPSHOT'
    assert match.major_version == '1'
    assert match.minor_version == '0.0'
    assert match.suffix == 'egg'
    assert match.lib_type == 'python-egg'


def test_filename_match_egg_snapshot_branch_with_py():
    match = FileNameMatch('new_library-1.0.0-SNAPSHOT-my-branch-py3.6.egg')
    assert match.library_name == 'new_library'
    assert match.version == '1.0.0-SNAPSHOT-my-branch'
    assert match.major_version == '1'
    assert match.minor_version == '0.0'
    assert match.suffix == 'egg'
    assert match.lib_type == 'python-egg'


def test_filename_match_egg():
    match = FileNameMatch('new_library-1.0.0.egg')
    assert match.library_name == 'new_library'
    assert match.version == '1.0.0'
    assert match.major_version == '1'
    assert match.minor_version == '0.0'
    assert match.suffix == 'egg'
    assert match.lib_type == 'python-egg'


def test_filename_match_egg_snapshot():
    match = FileNameMatch('new_library-1.0.0-SNAPSHOT.egg')
    assert match.library_name == 'new_library'
    assert match.version == '1.0.0-SNAPSHOT'
    assert match.major_version == '1'
    assert match.minor_version == '0.0'
    assert match.suffix == 'egg'
    assert match.lib_type == 'python-egg'


def test_filename_match_egg_snapshot_branch():
    match = FileNameMatch('new_library-1.0.0-SNAPSHOT-my-branch.egg')
    assert match.library_name == 'new_library'
    assert match.version == '1.0.0-SNAPSHOT-my-branch'
    assert match.major_version == '1'
    assert match.minor_version == '0.0'
    assert match.suffix == 'egg'
    assert match.lib_type == 'python-egg'


def test_filename_match_jar():
    match = FileNameMatch('new_library-1.0.0.jar')
    assert match.library_name == 'new_library'
    assert match.version == '1.0.0'
    assert match.major_version == '1'
    assert match.minor_version == '0.0'
    assert match.suffix == 'jar'
    assert match.lib_type == 'java-jar'


def test_filename_match_jar_snapshot():
    match = FileNameMatch('new_library-1.0.0-SNAPSHOT.jar')
    assert match.library_name == 'new_library'
    assert match.version == '1.0.0-SNAPSHOT'
    assert match.major_version == '1'
    assert match.minor_version == '0.0'
    assert match.suffix == 'jar'
    assert match.lib_type == 'java-jar'


def test_filename_match_jar_snapshot_branch():
    match = FileNameMatch('new_library-1.0.0-SNAPSHOT-my-branch.jar')
    assert match.library_name == 'new_library'
    assert match.version == '1.0.0-SNAPSHOT-my-branch'
    assert match.major_version == '1'
    assert match.minor_version == '0.0'
    assert match.suffix == 'jar'
    assert match.lib_type == 'java-jar'


def test_filename_match_wrong_file_type():
    with pytest.raises(FileNameError) as err:
        FileNameMatch('test-library-1.0.3.zip')
        assert err.filename == 'test-library-1.0.3.zip'


def test_filename_match_garbage_version():
    with pytest.raises(FileNameError) as err:
        FileNameMatch('test-library-1.0.3-askjdhfa.egg')
        assert err.filename == 'test-library-1.0.3-askjdhfa.egg'


def test_filename_match_equal():
    match_1 = FileNameMatch('test-library-1.0.3.egg')
    match_2 = FileNameMatch('test-library-1.0.3-py3.5.egg')
    assert match_1 == match_2


def test_filename_match_not_equal():
    match_1 = FileNameMatch('test-library-1.0.3.egg')
    match_2 = FileNameMatch('test-library-1.0.3-SNAPSHOT.egg')
    assert match_1 != match_2


def test_filename_match_should_replace():
    match_1 = FileNameMatch('test-library-1.1.3.egg')
    match_2 = FileNameMatch('test-library-1.0.3.egg')
    assert match_1.replace_version(match_2)


def test_filename_match_should_replace_snapshot():
    match_1 = FileNameMatch('test-library-1.1.3.egg')
    match_2 = FileNameMatch('test-library-1.0.3-SNAPSHOT.egg')
    assert match_1.replace_version(match_2)


def test_filename_match_should_not_replace_snapshot():
    match_1 = FileNameMatch('test-library-1.1.3.egg')
    match_2 = FileNameMatch('test-library-0.0.3-SNAPSHOT.egg')
    assert not match_1.replace_version(match_2)
