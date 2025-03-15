import json
import os
from typing import Dict, List, Any
from anthropic import Anthropic
from manual_mcp_dataretrieval import MCPDataRetrieval

class MCPAgent:
    """An agent that uses MCP to interact with the dataretrieval library."""

    def __init__(self, api_key: str = None):
        # Initialize the MCP wrapper
        self.mcp_wrapper = MCPDataRetrieval()

        # Initialize the LLM client
        self.client = Anthropic(api_key=api_key or os.environ.get("ANTHROPIC_API_KEY"))

        # Initialize conversation history
        self.messages = []

    def process_query(self, query: str) -> str:
        """Process a user query and interact with the dataretrieval library."""

        # Add user message to history
        self.messages.append({"role": "user", "content": query})

        # Create MCP context
        mcp_context = self.mcp_wrapper.format_mcp_context(messages=self.messages)

        # Create the prompt for the LLM with the MCP context
        prompt = self._create_prompt(query, mcp_context)

        # Call the LLM
        response = self.client.messages.create(
            model="claude-3-haiku-20240307",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )

        # Extract function calls if any
        assistant_message = response.content[0].text
        function_calls = self._extract_function_calls(assistant_message)

        # Process function calls and get results
        if function_calls:
            results = []
            for call in function_calls:
                function_name = call.get("name")
                params = call.get("parameters", {})

                # Execute the function
                result = self.mcp_wrapper.call_function(function_name, params)
                results.append({"function": function_name, "result": result})

            # Create a new prompt with function results
            prompt_with_results = self._create_prompt_with_results(query, mcp_context, results)

            # Call the LLM again with the results
            final_response = self.client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt_with_results}],
            )

            # Update conversation history
            self.messages.append({"role": "assistant", "content": final_response.content[0].text})

            return final_response.content[0].text
        else:
            # No function calls needed, return the original response
            self.messages.append({"role": "assistant", "content": assistant_message})
            return assistant_message

    def _create_prompt(self, query: str, mcp_context: Dict[str, Any]) -> str:
        """Create a prompt for the LLM with MCP context."""
        return f"""
        You are an AI assistant that helps users access and analyze USGS water data.

        MCP CONTEXT:
        {json.dumps(mcp_context, indent=2)}

        INSTRUCTIONS:
        1. If you need to access water data, use the functions defined in the MCP context.
        2. Format function calls as JSON objects inside <function_call></function_call> tags.
        3. Explain to the user what you're doing and provide insights about the data.

        USER QUERY:
        {query}

        Your response:
        """

    def _create_prompt_with_results(self, query: str, mcp_context: Dict[str, Any],
                                  results: List[Dict[str, Any]]) -> str:
        """Create a prompt that includes function call results."""
        return f"""
        You are an AI assistant that helps users access and analyze USGS water data.

        MCP CONTEXT:
        {json.dumps(mcp_context, indent=2)}

        FUNCTION CALL RESULTS:
        {json.dumps(results, indent=2)}

        INSTRUCTIONS:
        1. Based on the function call results, provide a helpful response to the user's query.
        2. Explain the data and provide insights where possible.
        3. If the data shows any interesting patterns or anomalies, point them out.
        4. If there were any errors in the function calls, explain them to the user.

        USER QUERY:
        {query}

        Your response:
        """

    def _extract_function_calls(self, text: str) -> List[Dict[str, Any]]:
        """Extract function calls from the assistant's response."""
        import re

        function_calls = []
        pattern = r'<function_call>(.*?)</function_call>'
        matches = re.findall(pattern, text, re.DOTALL)

        for match in matches:
            try:
                function_call = json.loads(match.strip())
                function_calls.append(function_call)
            except json.JSONDecodeError:
                # Invalid JSON, skip this match
                continue

        return function_calls


# Example usage
if __name__ == "__main__":
    agent = MCPAgent()

    # Example queries
    queries = [
        "Please summarize water use in the state of PA in 2015?",
        "What is the average water temperature in the Mississippi River?",
        "Show me the water flow rate in the Colorado River in 2020.",
        "What is the water quality in the Great Lakes?",
        "How does water usage vary seasonally in Washington DC?",
    ]

    for query in queries:
        print(f"\nQUERY: {query}")
        print("-" * 50)
        response = agent.process_query(query)
        print(f"RESPONSE:\n{response}")
        print("=" * 80)