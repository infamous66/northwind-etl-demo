import sys
import os
import subprocess
import unittest

class TestCommandLineArguments(unittest.TestCase):
    
    def test_default_arguments(self):
        """Test that default arguments work correctly"""
        cmd = [sys.executable, "../migrator.py", "--help"]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        self.assertEqual(result.returncode, 0)
        self.assertIn("--batch-size", result.stdout)
        self.assertIn("--num-batches", result.stdout)
    
    def test_invalid_batch_size(self):
        """Test that invalid batch size is rejected"""
        cmd = [sys.executable, "../migrator.py", "--batch-size", "0"]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("batch-size must be greater than 0", result.stderr)
    
    def test_invalid_num_batches(self):
        """Test that invalid number of batches is rejected"""
        cmd = [sys.executable, "../migrator.py", "--num-batches", "-1"]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("num-batches must be greater than 0", result.stderr)
    
    def test_valid_arguments(self):
        """Test that valid arguments are accepted"""
        cmd = [sys.executable, "../migrator.py", "--batch-size", "500", "--num-batches", "4"]
        result = subprocess.run(cmd, capture_output=True, text=True, cwd=os.path.dirname(__file__))
        
        # The script will fail because databases aren't running, but it should accept the arguments
        # We check that it doesn't fail due to argument parsing
        self.assertNotIn("batch-size must be greater than 0", result.stderr)
        self.assertNotIn("num-batches must be greater than 0", result.stderr)

if __name__ == "__main__":
    unittest.main()
