import json
from typing import Dict, List, Any, Optional, Union
import dataretrieval.nwis as nwis
import pandas as pd
import mcp
from mcp.server.fastmcp import FastMCP

class MCPDataRetrieval:
    """
    Model Context Protocol (MCP) wrapper for the dataretrieval Python library.
    This wrapper provides MCP-compliant interfaces to access USGS water data.
    """

    def __init__(self):
        """
        Initialize the MCP wrapper.
        """
        pass

    def _format_dataframe_result(self, df: pd.DataFrame, message: str) -> Dict[str, Any]:
        """
        Format a pandas DataFrame into a standardized result structure.

        Args:
            df (pd.DataFrame): The DataFrame to format
            message (str): A message to include in the result

        Returns:
            Dict[str, Any]: Formatted result
        """
        # Clean the DataFrame
        df.dropna(axis=1, how='all', inplace=True)
        df = df.loc[:, (df != '-').any(axis=0)]

        # Format for result
        return {
            "status": "success",
            "column_names": df.columns.tolist(),
            "data": df.values.tolist(),
            "message": message
        }

    async def get_site_data(self, site_code: str) -> Dict[str, Any]:
        """
        Get information about a specific USGS water monitoring site.

        Args:
            site_code (str): USGS site code (e.g., '09380000')

        Returns:
            Dict[str, Any]: Site information
        """
        try:
            site_info = nwis.get_record(sites=site_code, service="site")
            if not site_info.empty:
                return self._format_dataframe_result(
                    site_info,
                    f"Successfully retrieved data for site {site_code}"
                )
            else:
                return {"status": "error", "message": f"No data found for site {site_code}"}
        except Exception as e:
            return {"status": "error", "message": f"Error retrieving site data: {str(e)}"}

    async def get_daily_values(self,
                         site_code: str,
                         parameter_code: Optional[str] = None,
                         statCd: Optional[str] = None,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get daily values of water data.

        Args:
            site_code (str): USGS site code
            parameter_code (str, optional): USGS parameter code (e.g., '00060' for discharge)
            statCd (str, optional): USGS statistic code
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format

        Returns:
            Dict[str, Any]: Daily values data
        """
        try:
            daily_data, md = nwis.get_dv(
                sites=site_code,
                parameterCd=parameter_code,
                statCd=statCd,
                start=start_date,
                end=end_date
            )
            if not daily_data.empty:
                result = self._format_dataframe_result(
                    daily_data,
                    f"Successfully retrieved daily values for site {site_code}"
                )
                return result
            else:
                return {
                    "status": "error",
                    "message": "No daily values found for the specified parameters"
                }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error retrieving daily values: {str(e)}"
            }

    async def get_instantaneous_values(self,
                                site_code: str,
                                parameter_code: str,
                                start_date: Optional[str] = None,
                                end_date: Optional[str] = None) -> Dict[str, Any]:
        """
        Get instantaneous values of water data.

        Args:
            site_code (str): USGS site code
            parameter_code (str): USGS parameter code (e.g., '00060' for discharge)
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format

        Returns:
            Dict[str, Any]: Instantaneous values data
        """
        try:
            iv_data, md = nwis.get_iv(
                sites=site_code,
                parameterCd=parameter_code,
                start=start_date,
                end=end_date
            )
            if not iv_data.empty:
                result = self._format_dataframe_result(
                    iv_data,
                    f"Successfully retrieved instantaneous values for site {site_code}"
                )
                return result
            else:
                return {
                    "status": "error",
                    "message": "No instantaneous values found for the specified parameters"
                }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Error retrieving instantaneous values: {str(e)}"
            }

    async def get_discharge_measurements(self,
                                  sites: str,
                                  start: Optional[str] = None,
                                  end: Optional[str] = None) -> Dict[str, Any]:
        """
        Get discharge measurements from the waterdata service.

        Args:
            sites (str): USGS site code(s), comma-separated
            start (str, optional): Start date in YYYY-MM-DD format
            end (str, optional): End date in YYYY-MM-DD format

        Returns:
            Dict[str, Any]: Discharge measurements data
        """
        try:
            # Split sites into list if comma-separated
            sites_list = sites.split(",") if isinstance(sites, str) else sites

            df, md = nwis.get_discharge_measurements(sites=sites_list, start=start, end=end)
            result = self._format_dataframe_result(
                df,
                f"Retrieved {len(df)} discharge measurements"
            )
            return result
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def get_discharge_peaks(self,
                           sites: str,
                           start: Optional[str] = None,
                           end: Optional[str] = None) -> Dict[str, Any]:
        """
        Get discharge peaks from the waterdata service.

        Args:
            sites (str): USGS site code(s), comma-separated
            start (str, optional): Start date in YYYY-MM-DD format
            end (str, optional): End date in YYYY-MM-DD format

        Returns:
            Dict[str, Any]: Discharge peaks data
        """
        try:
            sites_list = sites.split(",") if isinstance(sites, str) else sites
            df, md = nwis.get_discharge_peaks(sites=sites_list, start=start, end=end)

            result = self._format_dataframe_result(
                df,
                f"Retrieved {len(df)} discharge peaks"
            )
            return result
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def get_gwlevels(self,
                    sites: str,
                    start: Optional[str] = None,
                    end: Optional[str] = None) -> Dict[str, Any]:
        """
        Get groundwater levels from the waterdata service.

        Args:
            sites (str): USGS site code(s), comma-separated
            start (str, optional): Start date in YYYY-MM-DD format
            end (str, optional): End date in YYYY-MM-DD format

        Returns:
            Dict[str, Any]: Groundwater levels data
        """
        try:
            sites_list = sites.split(",") if isinstance(sites, str) else sites
            df, md = nwis.get_gwlevels(sites=sites_list, start=start, end=end)

            result = self._format_dataframe_result(
                df,
                f"Retrieved {len(df)} groundwater level records"
            )
            return result
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def get_ratings(self,
                   site: str,
                   file_type: str = "base") -> Dict[str, Any]:
        """
        Get rating table for an active USGS streamgage.

        Args:
            site (str): USGS site code
            file_type (str, optional): File type (base, corr, exsa)

        Returns:
            Dict[str, Any]: Rating data
        """
        try:
            df, md = nwis.get_ratings(site=site, file_type=file_type)

            result = self._format_dataframe_result(
                df,
                f"Retrieved {len(df)} rating records for site {site}"
            )
            return result
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def what_sites(self,
                  stateCd: Optional[str] = None,
                  siteType: Optional[str] = None,
                  county: Optional[str] = None,
                  huc: Optional[str] = None) -> Dict[str, Any]:
        """
        Search NWIS for sites within a region with specific data.

        Args:
            stateCd (str, optional): Two-letter state code (e.g., 'CA')
            siteType (str, optional): Type of site (e.g., 'ST' for stream)
            county (str, optional): County code
            huc (str, optional): Hydrologic Unit Code

        Returns:
            Dict[str, Any]: Matching sites data
        """
        try:
            params = {}
            if stateCd: params["stateCd"] = stateCd
            if siteType: params["siteType"] = siteType
            if county: params["county"] = county
            if huc: params["huc"] = huc

            df, md = nwis.what_sites(**params)

            result = self._format_dataframe_result(
                df,
                f"Found {len(df)} sites matching the criteria"
            )
            return result
        except Exception as e:
            return {"status": "error", "message": str(e)}


# Example usage
if __name__ == "__main__":
    # Create the MCP wrapper instance
    data_retrieval = MCPDataRetrieval()

    # Create a FastMCP instance with the data retrieval tools
    tools = {
        "get_site_data": data_retrieval.get_site_data,
        "get_daily_values": data_retrieval.get_daily_values,
        "get_instantaneous_values": data_retrieval.get_instantaneous_values,
        "get_discharge_measurements": data_retrieval.get_discharge_measurements,
        "get_discharge_peaks": data_retrieval.get_discharge_peaks,
        "get_gwlevels": data_retrieval.get_gwlevels,
        "get_ratings": data_retrieval.get_ratings,
        "what_sites": data_retrieval.what_sites
    }

    # Start the FastMCP server
    server = FastMCP(tools=tools, host="127.0.0.1", port=8000)
    server.run()