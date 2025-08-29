"""Module manager for executing different components"""

import json
import subprocess
import os
from typing import Dict, Any

class ModuleResult:
    """Result container for module execution"""

    def __init__(self, success: bool, data: Dict[str, Any] = None, error: str = None):
        """Initialize module result

        Args:
            success: Whether the module execution was successful
            Dict: Data returned by the module
            error: Error message if execution failed
        """

        self.success = success
        self.data = data or {}
        self.error = error


class ModuleInterface:
    """Interface for executing modules either as Python functions or external executables (for future C/C++/Rust implementations)"""

    @staticmethod
    def execute_module(module_name: str, args: Dict[str, Any]) -> ModuleResult:
        """Execute a module

        Args:
            module_name: Name of the module to execute
            args: Arguments to pass to the module

        Returns:
            ModuleResult containing execution results
        """
        executable_path = f"modules/bin/{module_name}"
        if os.path.exists(executable_path):
            return ModuleInterface._execute_external_module(executable_path, args)
        else:
            return ModuleInterface._execute_python_module(module_name, args)

    @staticmethod
    def _execute_external_module(executable_path: str, args: Dict[str, Any]) -> ModuleResult:
        """Execute an external module

        Args:
            executable_path: Path to the executable
            args: Arguments to pass to the module

        Returns:
            ModuleResult containing execution results
        """
        try:
            input_json = json.dumps(args)
            result = subprocess.run(
                    [executable_path],
                    input=input_json,
                    text=True,
                    capture_output=True,
                    timeout=300
                )

            if result.returncode == 0:
                    output_data = json.loads(result.stdout) if result.stdout.strip() else {}
                    return ModuleResult(success=True, data=output_data)
            else:
                    return ModuleResult(success=False, error=result.stderr)

        except subprocess.TimeoutExpired:
            return ModuleResult(success=False, error="Module execution timed out")
        except json.JSONDecodeError:
            return ModuleResult(success=False, error="Invalid JSON response from module")
        except Exception as e:
            return ModuleResult(success=False, error=str(e))

    @staticmethod
    def _execute_python_module(module_name: str, args: Dict[str, Any]) -> ModuleResult:
        """Execute a Python module

        Args:
            module_name: Name of the Python module
            args: Arguments to pass to the module

        Returns:
            ModuleResult containing execution result
        """
        try:
            if module_name == 'file_loader':
                from modules.file_loader import execute
            elif module_name == 'db_connector':
                from modules.db_connector import execute
            elif module_name == 'analyzer':
                from modules.analyzer import execute
            elif module_name == 'reporter':
                from modules.reporter import execute
            else:
                return ModuleResult(success=False, error=f"Unknown module: {module_name}")

            result_data = execute(args)
            return ModuleResult(success=True, data=result_data)

        except Exception as e:
            return ModuleResult(success=False, error=str(e))


class ModuleManager:
    """Manager for executing modules through the module interface"""

    def execute_module(self, module_name: str, args: dict):
        """Execute module through module interface

        Args:
            module_name: Name of the module to execute
            args: Arguments to pass to the module

        Returns:
            ModuleResult containing execution results
        """
        return ModuleInterface.execute_module(module_name, args)

