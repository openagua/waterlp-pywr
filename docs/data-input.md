# Data input

This model is designed to integrate with [OpenAgua](https://www.openagua.org). General concepts related to entering data are described. Documentation about the specific syntax and functions is forthcoming.

# Resource type and variables


# Functions

## General concepts

This code is evaluated using the evaluate function found in evaluator.py. Essentially, a string representation of a function is created, with the entered code as the body of the function. All lines are indented appropriately as needed for Python. The function is then called by the evaluator and assigned to a variable. The function is run once per time step.

The last line of the user-entered code is automatically prepended with "return ", such that the user doesn't need to. This is most useful for simple cases, such as a constant value:

However, the evaluator also automatically detects the presence of a "return " in the last line, such that the user may also include a return as desired. So `return x` on the last line is the same as `x`. In many cases, including `return` is simply a matter of personal preference. But this is particularly useful if a return is nested in the last part of a conditional statement. To demonstrate, the following three versions of code input yield the exact same result when evaluated:

```python
if date.month in [6,7,8]:
    x = 0.5
else:
    x = 1
x
```

```python
if date.month in [6,7,8]:
    x = 0.5
else:
    x = 1
return x
```

```python
if date.month in [6,7,8]:
    return 0.5
else:
    return 1
```

This scheme enables the user to import, enter custom functions directly into the code, etc. In the future, this will also enable offering a range of custom Python functions \(Check **Writing Functions**\). It will also allow the user to create, store, re-use, and share custom functions.

The last three examples above should raise a question: where does "date" come from? OpenAgua will include several built-in variables available for use. For now, this only includes the date of the function call. In the future, however, this will expand to include others as needed.
