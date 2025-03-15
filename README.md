# MCP Wrapper to the USGS Python Data Retrieval Library

This repository contains MCP-compliant functions for retrieving data using the USGS Python `dataretrieval` package.
There are two versions of the MCP implemented:

1. `manual_mcp_dataretrieval.py`: A manual implementation of the MCP functions that interact with the `dataretrieval` package.
2. `mcp_dataretrieval.py`: An automated implementation of the MCP functions that interact with the `dataretrieval` package that utilizes the MCP Python SDK.

## Installation and Setup

### Dependencies

Dependencies are listed in both the `pyproject.toml` file and the `uv.lock` file.
The main dependencies are:

- "anthropic>=0.49.0",
- "dataretrieval>=1.0.11",
- "httpx>=0.28.1",
- "mcp[cli]>=1.3.0",
- "pandas>=2.2.3",

### Setup

Follow the Anthropic [QuickStart Guide](https://modelcontextprotocol.io/quickstart/server#core-mcp-concepts) for details on setting up an MCP server.

### Caveats

- I am running a Linux machine and haven't been able to test the MCP server implementation on "Claude for Desktop" as it is not yet supported on Linux.

I have tested the manual MCP implementation on my local machine and it works as expected; examples below.

## Example Prompts and Responses

The `example_agent.py` script demonstrates the use of the manual MCP implementation in `manual_mcp_dataretrieval.py` via a simple Agent that utilizes Claude 3 Haiku (claude-3-haiku-20240307) to process the user's question, select one or more functions available via the MCP tools, run the functions, and then summarize the results to answer the user's original question.

Below are some sample questions and responses generated via that workflow.
Note that there is more optimization that could be done in terms of either the prompting strategy (to be more specific about the sites and questions to query), as well as in the internals of the MCP functions (to minimize API results and be as token-conscious as possible).

---

**QUERY:** Please summarize water use in the state of PA in 2015?


**RESPONSE:**
Based on the water use data provided for Pennsylvania in 2015, here is a summary:

The total population of Pennsylvania in 2015 was 12.64 million. Of this, 9.78 million were served by public water supplies, with 5.76 million getting water from surface sources and 3.92 million from groundwater sources.

The total water withdrawals for public supply were 47.23 million gallons per day (Mgal/d), with 20.4 Mgal/d from groundwater and 42.91 Mgal/d from surface water. Around 63.31 Mgal/d was delivered to domestic users.

For domestic self-supplied users, an additional 9.78 Mgal/d was withdrawn, primarily from groundwater (9.18 Mgal/d).

Industrial withdrawals were 2.27 Mgal/d from groundwater and 0.01 Mgal/d from surface water, totaling 2.27 Mgal/d.

The largest water use sector was thermoelectric power generation, which withdrew 41.43 Mgal/d from surface water sources. Power generation consumed 33.59 Mgal/d of this water.

Other notable water uses include:
- Mining: 0.21 Mgal/d withdrawals
- Livestock: 0.39 Mgal/d withdrawals
- Aquaculture: 1.24 Mgal/d withdrawals

The data shows that Pennsylvania relies heavily on surface water for its public supply and thermoelectric power needs. Groundwater plays a more significant role for domestic self-supplied users. Overall, water withdrawals appear to be dominated by the thermoelectric power and public supply sectors.

No major data quality issues were identified in the function call results provided. The data provides a comprehensive breakdown of water use by sector and source, enabling a detailed analysis of Pennsylvania's water use patterns in 2015.

---

**QUERY:** What is the average water temperature in the Mississippi River?


**RESPONSE:**
Unfortunately, I do not have enough information to provide the average water temperature for the Mississippi River. The function calls made did not return any data related to water temperature in the Mississippi River.

The first function call to `what_sites` searched for USGS monitoring sites in Minnesota within the specified HUC (Hydrologic Unit Code) watershed, but this did not return any results. This suggests that the query parameters provided may not have been specific enough to find relevant sites on the Mississippi River.

The second function call to `get_daily_values` also did not return any data, likely because no specific site code or parameter code was provided to retrieve water temperature data.

To get the average water temperature for the Mississippi River, I would need more targeted information, such as:

1. The specific USGS monitoring site code(s) located along the Mississippi River.
2. The USGS parameter code for water temperature (likely '00010').
3. A date range to calculate the average over.

Without access to this more specific data, I am unable to provide the average water temperature for the Mississippi River. Please let me know if you can provide any of those details, and I can try to retrieve and analyze the relevant water temperature data.

---

**QUERY:** Show me the water flow rate in the Colorado River in 2020.


**RESPONSE:**
Based on the function call results, the water flow rate for the Colorado River at the USGS monitoring station 09415000 in 2020 is provided.

The data shows the daily mean discharge values (in cubic feet per second) for this site over the course of the year. A few key insights:

1. The discharge values fluctuate considerably throughout the year, ranging from a low of around 50 cfs to a high of over 1,800 cfs. This reflects the natural variability in the Colorado River's flow over the seasons.

2. The highest flows occurred in the spring and early summer, likely due to snowmelt runoff in the Colorado River basin. Peak flows reached over 1,800 cfs in late May/early June.

3. Discharge gradually declined from the peak spring flows to more moderate levels of around 100-200 cfs by late summer and autumn.

4. There are a few instances marked with an "e" flag, indicating that the discharge values were estimated rather than directly measured. This is common for stream gages, especially during high flow events.

Overall, the data provides a good overview of the 2020 discharge patterns for this important segment of the Colorado River. The significant seasonal variability is typical for this snowmelt-dominated river system. Let me know if you need any clarification or have additional questions!

---

**QUERY:** What is the water quality in the Great Lakes?

Traceback (most recent call last):

  ...

```
raise self._make_status_error_from_response(err.response) from None
anthropic.BadRequestError: Error code: 400 - {'type': 'error', 'error': {'type': 'invalid_request_error', 'message': 'prompt is too long: 210166 tokens > 200000 maximum'}}
```
