"""
Example usage of MiniModal.

To run this example:

1. Start the server:
   python -m minimodal.server.api
   
2. Start a worker (in another terminal):
   python -m minimodal.worker.executor
   
3. Run this example:
   python examples/hello.py
"""

from minimodal import App

# Create an app
app = App("hello-app")


# Define a simple function
@app.function()
def hello(name: str) -> str:
    """A simple greeting function."""
    return f"Hello, {name}!"


# Define a function with some computation
@app.function()
def fibonacci(n: int) -> int:
    """Compute nth fibonacci number (slow recursive version)."""
    if n <= 1:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)


# Define a function that uses numpy (would need requirements in real usage)
@app.function()
def sum_squares(numbers: list) -> float:
    """Sum of squares without numpy for simplicity."""
    return sum(x ** 2 for x in numbers)


if __name__ == "__main__":
    # Deploy all functions
    print("Deploying functions...")
    app.deploy()
    print()
    
    # Test local execution
    print("Local execution:")
    print(f"  hello('World') = {hello('World')}")
    print(f"  fibonacci(10) = {fibonacci(10)}")
    print()
    
    # Test remote execution
    print("Remote execution:")
    result = hello.remote("MiniModal")
    print(f"  hello.remote('MiniModal') = {result}")
    
    result = fibonacci.remote(10)
    print(f"  fibonacci.remote(10) = {result}")
    
    result = sum_squares.remote([1, 2, 3, 4, 5])
    print(f"  sum_squares.remote([1,2,3,4,5]) = {result}")
