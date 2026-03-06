"""
Tests for case-sensitivity conflict detector.

Tests cover:
- Detection of case-only conflicts from Git
- Handling of edge cases (symlinks, ignored directories)
- Report generation (JSON, text)
- CLI functionality
"""

import os
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

from app.tools.case_sensitivity_detector import (
    scan_directory,
    _check_case_conflicts,
    _check_git_conflicts,
    get_git_files,
    generate_report,
    format_report_text,
    check_git_staged_files
)
from app.tools.case_detector_cli import main as cli_main


class TestCaseSensitivityDetector(unittest.TestCase):
    """Tests for case sensitivity detector module."""

    def test_no_conflicts_in_empty_directory(self):
        """Test that empty directory returns no conflicts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch('app.tools.case_sensitivity_detector.get_git_files', return_value=[]):
                conflicts = scan_directory(tmpdir)
                self.assertEqual(len(conflicts), 0)

    def test_no_conflicts_with_single_file(self):
        """Test that single file returns no conflicts."""
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(tmpdir, "user.py").write_text("# test")
            with patch('app.tools.case_sensitivity_detector.get_git_files', return_value=['user.py']):
                conflicts = scan_directory(tmpdir)
                self.assertEqual(len(conflicts), 0)

    def test_detects_case_duplicates_from_git(self):
        """Test detection of files differing only in case from Git."""
        with patch('app.tools.case_sensitivity_detector.get_git_files') as mock_git:
            mock_git.return_value = ['User.py', 'user.py']
            conflicts = _check_git_conflicts('.')
            
            self.assertEqual(len(conflicts), 1)
            self.assertEqual(len(conflicts[0]['files']), 2)
            self.assertIn("User.py", conflicts[0]['files'])
            self.assertIn("user.py", conflicts[0]['files'])
            self.assertEqual(conflicts[0]['severity'], 'high')

    def test_detects_multiple_case_duplicates_from_git(self):
        """Test detection of multiple conflicts in same directory from Git."""
        with patch('app.tools.case_sensitivity_detector.get_git_files') as mock_git:
            mock_git.return_value = ['Auth.py', 'auth.py', 'Config.py', 'config.py']
            conflicts = _check_git_conflicts('.')
            
            self.assertEqual(len(conflicts), 2)  # Two separate conflicts
            
            # Find the Auth conflict
            auth_conflict = [c for c in conflicts if 'Auth.py' in c['files']][0]
            self.assertEqual(len(auth_conflict['files']), 2)

    def test_detects_nested_conflicts_from_git(self):
        """Test detection of conflicts in nested directories from Git."""
        with patch('app.tools.case_sensitivity_detector.get_git_files') as mock_git:
            mock_git.return_value = ['app/services/Auth.py', 'app/services/auth.py']
            conflicts = _check_git_conflicts('.')
            
            self.assertEqual(len(conflicts), 1)
            self.assertIn('Auth.py', conflicts[0]['files'])
            self.assertIn('auth.py', conflicts[0]['files'])

    def test_ignores_ignored_extensions(self):
        """Test that ignored file extensions are skipped."""
        with patch('app.tools.case_sensitivity_detector.get_git_files') as mock_git:
            mock_git.return_value = ['Module.pyc', 'module.pyc', 'Module.py', 'module.py']
            conflicts = _check_git_conflicts('.')
            
            # Should only detect .py conflict, not .pyc
            self.assertEqual(len(conflicts), 1)
            pyc_files = [f for f in conflicts[0]['files'] if f.endswith('.pyc')]
            self.assertEqual(len(pyc_files), 0)

    def test_check_case_conflicts_function(self):
        """Test the single-directory conflict checker."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filenames = ["User.py", "user.py", "Auth.py"]
            
            conflict = _check_case_conflicts(tmpdir, filenames)
            
            self.assertIsNotNone(conflict)
            self.assertEqual(len(conflict['files']), 2)
            self.assertIn("User.py", conflict['files'])
            self.assertIn("user.py", conflict['files'])

    def test_check_case_conflicts_no_conflict(self):
        """Test that no false positives from single checker."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filenames = ["User.py", "Auth.py", "Config.py"]
            
            conflict = _check_case_conflicts(tmpdir, filenames)
            
            self.assertIsNone(conflict)

    def test_check_case_conflicts_empty_list(self):
        """Test with empty filenames list."""
        with tempfile.TemporaryDirectory() as tmpdir:
            conflict = _check_case_conflicts(tmpdir, [])
            self.assertIsNone(conflict)

    def test_generate_report_with_conflicts(self):
        """Test report generation with conflicts."""
        conflicts = [
            {
                'directory': 'app/services',
                'files': ['Auth.py', 'auth.py'],
                'severity': 'high'
            }
        ]
        
        report = generate_report(conflicts)
        
        self.assertEqual(report['total_conflicts'], 1)
        self.assertEqual(report['status'], 'FAILED')
        self.assertEqual(len(report['conflicts']), 1)

    def test_generate_report_without_conflicts(self):
        """Test report generation without conflicts."""
        conflicts = []
        
        report = generate_report(conflicts)
        
        self.assertEqual(report['total_conflicts'], 0)
        self.assertEqual(report['status'], 'PASSED')
        self.assertEqual(len(report['conflicts']), 0)

    def test_format_report_text_with_conflicts(self):
        """Test text formatting with conflicts."""
        report = {
            'total_conflicts': 1,
            'conflicts': [
                {
                    'directory': 'app',
                    'files': ['Config.py', 'config.py'],
                    'severity': 'high'
                }
            ],
            'status': 'FAILED'
        }
        
        text = format_report_text(report)
        
        self.assertIn('Conflicts Detected', text)
        self.assertIn('Config.py', text)
        self.assertIn('config.py', text)

    def test_format_report_text_without_conflicts(self):
        """Test text formatting without conflicts."""
        report = {
            'total_conflicts': 0,
            'conflicts': [],
            'status': 'PASSED'
        }
        
        text = format_report_text(report)
        
        self.assertIn('No case-sensitivity conflicts found', text)


class TestGitFunctions(unittest.TestCase):
    """Tests for Git-related functions."""

    @patch('subprocess.run')
    def test_get_git_files_success(self, mock_run):
        """Test getting files from git."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "file1.py\nfile2.py\n"
        
        files = get_git_files('.')
        
        self.assertEqual(len(files), 2)
        self.assertIn('file1.py', files)
        self.assertIn('file2.py', files)

    @patch('subprocess.run')
    def test_get_git_files_no_repo(self, mock_run):
        """Test get_git_files when not in a git repo."""
        mock_run.return_value.returncode = 128  # git error
        
        files = get_git_files('.')
        
        self.assertEqual(len(files), 0)

    @patch('subprocess.run')
    def test_check_git_staged_files_with_conflict(self, mock_run):
        """Test checking staged files for conflicts."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "app/user.py\napp/User.py\n"
        
        conflicts = check_git_staged_files('.')
        
        self.assertEqual(len(conflicts), 1)

    @patch('subprocess.run')
    def test_check_git_staged_files_no_conflict(self, mock_run):
        """Test checking staged files without conflicts."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "app/user.py\napp/config.py\n"
        
        conflicts = check_git_staged_files('.')
        
        self.assertEqual(len(conflicts), 0)

    @patch('subprocess.run')
    def test_check_git_staged_files_git_error(self, mock_run):
        """Test handling of git command error."""
        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ""
        
        conflicts = check_git_staged_files('.')
        
        self.assertEqual(len(conflicts), 0)

    @patch('subprocess.run')
    def test_check_git_staged_files_empty(self, mock_run):
        """Test with no staged files."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        
        conflicts = check_git_staged_files('.')
        
        self.assertEqual(len(conflicts), 0)


class TestCaseSensitivityCLI(unittest.TestCase):
    """Tests for CLI functionality."""

    @patch('app.tools.case_sensitivity_detector.get_git_files')
    def test_cli_scan_directory_with_conflicts(self, mock_git):
        """Test CLI scan of directory with conflicts."""
        mock_git.return_value = ['User.py', 'user.py']
        
        with tempfile.TemporaryDirectory() as tmpdir:
            exit_code = cli_main([tmpdir])
            self.assertEqual(exit_code, 1)  # Conflicts found

    @patch('app.tools.case_sensitivity_detector.get_git_files')
    def test_cli_text_report(self, mock_git):
        """Test CLI text report output."""
        mock_git.return_value = ['User.py', 'user.py']
        
        with tempfile.TemporaryDirectory() as tmpdir:
            exit_code = cli_main([tmpdir, '--report', 'text'])
            self.assertEqual(exit_code, 1)

    @patch('app.tools.case_sensitivity_detector.get_git_files')
    def test_cli_json_report(self, mock_git):
        """Test CLI JSON report output."""
        mock_git.return_value = ['Config.py', 'config.py']
        
        with tempfile.TemporaryDirectory() as tmpdir:
            exit_code = cli_main([tmpdir, '--report', 'json'])
            self.assertEqual(exit_code, 1)

    @patch('app.tools.case_sensitivity_detector.get_git_files')
    def test_cli_save_to_file(self, mock_git):
        """Test CLI saving report to file."""
        mock_git.return_value = ['File.py', 'file.py']
        
        with tempfile.TemporaryDirectory() as tmpdir:
            report_file = Path(tmpdir) / "report.json"
            
            exit_code = cli_main([
                tmpdir,
                '--report', 'json',
                '--output', str(report_file)
            ])
            
            self.assertEqual(exit_code, 1)
            self.assertTrue(report_file.exists())
            
            content = json.loads(report_file.read_text())
            self.assertEqual(content['status'], 'FAILED')

    @patch('app.tools.case_sensitivity_detector.get_git_files')
    def test_cli_no_conflicts(self, mock_git):
        """Test CLI with no conflicts found."""
        mock_git.return_value = ['file.py']
        
        with tempfile.TemporaryDirectory() as tmpdir:
            exit_code = cli_main([tmpdir])
            self.assertEqual(exit_code, 0)  # No conflicts

    def test_cli_default_path(self):
        """Test CLI with default current directory."""
        exit_code = cli_main([])
        # Just verify it runs without error
        self.assertIn(exit_code, [0, 1])

    @patch('subprocess.run')
    def test_cli_git_staged_option(self, mock_run):
        """Test CLI with --git-staged option."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "app/user.py\napp/User.py\n"
        
        exit_code = cli_main(['.', '--git-staged'])
        self.assertEqual(exit_code, 1)


if __name__ == '__main__':
    unittest.main()
