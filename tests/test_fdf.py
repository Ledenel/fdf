#!/usr/bin/env python

"""Tests for `fdf` package."""

import pytest

from click.testing import CliRunner

from fdf import fdf
from fdf import cli


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string


def test_command_line_interface():
    """Test the CLI."""
    runner = CliRunner()
    result = runner.invoke(cli.main)
    assert result.exit_code == 0
    assert 'fdf.cli.main' in result.output
    help_result = runner.invoke(cli.main, ['--help'])
    assert help_result.exit_code == 0
    assert '--help  Show this message and exit.' in help_result.output


def test_from_path():
    array_info = fdf.ArrayInfo.from_path("test#%x.__categories__.txt")
    assert array_info.backend == "txt"
    assert array_info.prefixes == ("test--x", "__categories__")

def test_from_gz_path():
    array_info = fdf.ArrayInfo.from_path("test#%x.__categories__.txt.gz")
    assert array_info.backend == "txt"
    assert array_info.compression == "gz"
    assert array_info.prefixes == ("test--x", "__categories__")
