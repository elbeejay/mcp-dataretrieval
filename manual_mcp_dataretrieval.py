import json
from typing import Dict, List, Any, Optional, Union
import dataretrieval.nwis as nwis
import pandas as pd

class MCPDataRetrieval:
    """
    Model Context Protocol (MCP) wrapper for the dataretrieval Python library.
    This wrapper provides MCP-compliant interfaces to access USGS water data.

    The class encapsulates various methods to retrieve water data from USGS
    and formats the responses according to the MCP specification.
    """

    def __init__(self):
        """Initialize the function mapping dictionary."""
        self.functions = {
            "get_site_data": self.get_site_data,
            "get_daily_values": self.get_daily_values,
            "get_instantaneous_values": self.get_instantaneous_values,
            "get_discharge_measurements": self.get_discharge_measurements,
            "get_discharge_peaks": self.get_discharge_peaks,
            "get_gwlevels": self.get_gwlevels,
            "get_info": self.get_info,
            "get_pmcodes": self.get_pmcodes,
            "get_ratings": self.get_ratings,
            "get_record": self.get_record,
            "get_stats": self.get_stats,
            "get_water_use": self.get_water_use,
            "what_sites": self.what_sites
        }

    def get_mcp_functions(self) -> List[Dict[str, Any]]:
        """
        Returns the MCP-compliant function definitions.

        Returns:
            List[Dict[str, Any]]: List of function definitions in MCP format
        """
        return [
            {
                "name": "get_site_data",
                "description": "Get information about a specific USGS water monitoring site",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "site_code": {
                            "type": "string",
                            "description": "USGS site code (e.g., '09380000')"
                        }
                    },
                    "required": ["site_code"]
                }
            },
            {
                "name": "get_daily_values",
                "description": "Get daily values of water data",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "site_code": {
                            "type": "string",
                            "description": "USGS site code"
                        },
                        "parameter_code": {
                            "type": "string",
                            "description": "USGS parameter code (e.g., '00060' for discharge)"
                        },
                        "statCd": {
                            "type": "string",
                            "description": "USGS statistic code"
                        },
                        "start_date": {
                            "type": "string",
                            "description": "Start date in YYYY-MM-DD format"
                        },
                        "end_date": {
                            "type": "string",
                            "description": "End date in YYYY-MM-DD format"
                        }
                    },
                    "required": ["site_code"]
                }
            },
            {
                "name": "get_instantaneous_values",
                "description": "Get instantaneous values of water data",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "site_code": {
                            "type": "string",
                            "description": "USGS site code"
                        },
                        "parameter_code": {
                            "type": "string",
                            "description": "USGS parameter code (e.g., '00060' for discharge)"
                        },
                        "start_date": {
                            "type": "string",
                            "description": "Start date in YYYY-MM-DD format"
                        },
                        "end_date": {
                            "type": "string",
                            "description": "End date in YYYY-MM-DD format"
                        }
                    },
                    "required": ["site_code", "parameter_code"]
                }
            },
            {
                "name": "get_discharge_measurements",
                "description": "Get discharge measurements from the waterdata service",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sites": {
                            "type": "string",
                            "description": "USGS site code(s)"
                        },
                        "start": {
                            "type": "string",
                            "description": "Start date in YYYY-MM-DD format"
                        },
                        "end": {
                            "type": "string",
                            "description": "End date in YYYY-MM-DD format"
                        }
                    },
                    "required": ["sites"]
                }
            },
            {
                "name": "get_discharge_peaks",
                "description": "Get discharge peaks from the waterdata service",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sites": {
                            "type": "string",
                            "description": "USGS site code(s)"
                        },
                        "start": {
                            "type": "string",
                            "description": "Start date in YYYY-MM-DD format"
                        },
                        "end": {
                            "type": "string",
                            "description": "End date in YYYY-MM-DD format"
                        }
                    },
                    "required": ["sites"]
                }
            },
            {
                "name": "get_gwlevels",
                "description": "Get groundwater levels from the waterdata service",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sites": {
                            "type": "string",
                            "description": "USGS site code(s)"
                        },
                        "start": {
                            "type": "string",
                            "description": "Start date in YYYY-MM-DD format"
                        },
                        "end": {
                            "type": "string",
                            "description": "End date in YYYY-MM-DD format"
                        }
                    },
                    "required": ["sites"]
                }
            },
            {
                "name": "get_ratings",
                "description": "Get rating table for an active USGS streamgage",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "site": {
                            "type": "string",
                            "description": "USGS site code"
                        },
                        "file_type": {
                            "type": "string",
                            "description": "File type (base, corr, exsa)",
                            "default": "base"
                        }
                    },
                    "required": ["site"]
                }
            },
            {
                "name": "what_sites",
                "description": "Search NWIS for sites within a region with specific data",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "stateCd": {
                            "type": "string",
                            "description": "Two-letter state code (e.g., 'CA')"
                        },
                        "siteType": {
                            "type": "string",
                            "description": "Type of site (e.g., 'ST' for stream)"
                        },
                        "county": {
                            "type": "string",
                            "description": "County code"
                        },
                        "huc": {
                            "type": "string",
                            "description": "Hydrologic Unit Code"
                        }
                    }
                }
            },
            {
                "name": "get_stats",
                "description": "Get water services statistics information",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sites": {
                            "type": "string",
                            "description": "USGS site code(s)"
                        },
                        "parameterCd": {
                            "type": "string",
                            "description": "USGS parameter code (e.g., '00060' for discharge)"
                        },
                        "statReportType": {
                            "type": "string",
                            "description": "Type of statistical report"
                        },
                        "statTypeCd": {
                            "type": "string",
                            "description": "Type of statistical data"
                        },
                    }
                }
            },
            {
                "name": "get_info",
                "description": "Get site description information from NWIS",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "sites": {
                            "type": "string",
                            "description": "USGS site code(s)"
                        },
                        "stateCd": {
                            "type": "string",
                            "description": "Two-letter state code (e.g., 'CA')"
                        },
                        "huc": {
                            "type": "string",
                            "description": "Hydrologic Unit Code(s)"
                        },
                        "bBox": {
                            "type": "string",
                            "description": "Bounding box coordinates (minx,miny,maxx,maxy)"
                        },
                        "countyCd": {
                            "type": "string",
                            "description": "County code(s)"
                        },
                        "startDt": {
                            "type": "string",
                            "description": "Start date in YYYY-MM-DD format"
                        },
                        "endDt": {
                            "type": "string",
                            "description": "End date in YYYY-MM-DD format"
                        },
                        "period": {
                            "type": "string",
                            "description": "Period of record (e.g., 'P7D' for 7 days)"
                        },
                        "modifiedSince": {
                            "type": "string",
                            "description": "Modified since date in YYYY-MM-DD format"
                        },
                        "parameterCd": {
                            "type": "string",
                            "description": "USGS parameter code (e.g., '00060' for discharge)"
                        },
                        "siteType": {
                            "type": "string",
                            "description": "Type of site (e.g., 'ST' for stream)"
                        },
                        "siteOutput": {
                            "type": "string",
                            "description": "Site output format"
                        },
                        "seriesCatalogOutput": {
                            "type": "string",
                            "description": "Series catalog output format"
                        },
                    }
                }
            },
            {
                "name": "get_pmcodes",
                "description": "Get NWIS parameter codes",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "parameterCd": {
                            "type": "string",
                            "description": "USGS parameter code"
                        }
                    }
                }
            },
            {
                "name": "get_water_use",
                "description": "Get water use data from USGS (NWIS)",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "years": {
                            "type": "string",
                            "description": "Years to retrieve data for"
                        },
                        "state": {
                            "type": "string",
                            "description": "Two-letter state code (e.g., 'CA')"
                        },
                        "counties": {
                            "type": "string",
                            "description": "County codes"
                        },
                        "categories": {
                            "type": "string",
                            "description": "Water use categories"
                        }
                    }
                }
            }
        ]

    def _format_response(self, status: str, column_names: Any = None, data: Any = None, message: str = None, metadata: Any = None) -> Dict[str, Any]:
        """
        Format a standardized response dictionary.

        Args:
            status (str): Status of the operation ('success' or 'error')
            data (Any, optional): Data to return
            column_names (Any, optional): Column names for the data
            message (str, optional): Message to include in the response
            metadata (Any, optional): Additional metadata

        Returns:
            Dict[str, Any]: Formatted response
        """
        response = {"status": status}

        if data is not None:
            response["data"] = data

        if column_names is not None:
            response["column_names"] = column_names

        if message is not None:
            response["message"] = message

        if metadata is not None:
            response["metadata"] = metadata

        return response

    def get_site_data(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get information about a specific USGS water monitoring site.

        Args:
            params (Dict[str, Any]): Parameters containing site_code

        Returns:
            Dict[str, Any]: Response with site information
        """
        site_code = params.get("site_code")
        if not site_code:
            return self._format_response("error", message="Site code is required")

        try:
            site_info = nwis.get_record(sites=site_code, service="site")
            # drop columns with all NaN values
            site_info.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            site_info = site_info.loc[:, (site_info != '-').any(axis=0)]

            if not site_info.empty:
                column_names = site_info.columns.tolist()
                data_values = site_info.values.tolist()
                return self._format_response(
                    "success",
                    column_names=column_names,
                    data=data_values,
                    message=f"Successfully retrieved data for site {site_code}"
                )
            else:
                return self._format_response("error", message=f"No data found for site {site_code}")
        except Exception as e:
            return self._format_response("error", message=f"Error retrieving site data: {str(e)}")


    def get_daily_values(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get daily values of water data.

        Args:
            params (Dict[str, Any]): Parameters containing site_code, and optionally parameter_code, statCd, start_date, end_date

        Returns:
            Dict[str, Any]: Response with daily values
        """
        site_code = params.get("site_code")
        parameter_code = params.get("parameter_code")
        statCd = params.get("statCd")

        if not site_code:
            return self._format_response(
                "error",
                message="site_code is required"
            )

        start_date = params.get("start_date")
        end_date = params.get("end_date")

        try:
            daily_data, md = nwis.get_dv(
                sites=site_code,
                parameterCd=parameter_code,
                statCd=statCd,
                start=start_date,
                end=end_date
            )
            # drop columns with all NaN values
            daily_data.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            daily_data = daily_data.loc[:, (daily_data != '-').any(axis=0)]

            if not daily_data.empty:
                column_names = daily_data.columns.tolist()
                data_values = daily_data.values.tolist()
                return self._format_response(
                    "success",
                    column_names=column_names,
                    data=data_values,
                    message=f"Successfully retrieved daily values for site {site_code}"
                )
            else:
                return self._format_response(
                    "error",
                    message="No daily values found for the specified parameters"
                )
        except Exception as e:
            return self._format_response(
                "error",
                message=f"Error retrieving daily values: {str(e)}"
            )

    def get_instantaneous_values(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get instantaneous values of water data.

        Args:
            params (Dict[str, Any]): Parameters containing site_code, parameter_code,
                                     start_date, and end_date

        Returns:
            Dict[str, Any]: Response with instantaneous values
        """
        site_code = params.get("site_code")
        parameter_code = params.get("parameter_code")
        start_date = params.get("start_date")
        end_date = params.get("end_date")

        if not all([site_code, parameter_code]):
            return self._format_response(
                "error",
                message="site_code and parameter_code are both required"
            )

        try:
            iv_data, md = nwis.get_iv(
                sites=site_code,
                parameterCd=parameter_code,
                start=start_date,
                end=end_date
            )
            # drop columns with all NaN values
            iv_data.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            iv_data = iv_data.loc[:, (iv_data != '-').any(axis=0)]

            if not iv_data.empty:
                column_names = iv_data.columns.tolist()
                data_values = iv_data.values.tolist()
                return self._format_response(
                    "success",
                    column_names=column_names,
                    data=data_values,
                    message=f"Successfully retrieved instantaneous values for site {site_code}"
                )
            else:
                return self._format_response(
                    "error",
                    message="No instantaneous values found for the specified parameters"
                )
        except Exception as e:
            return self._format_response(
                "error",
                message=f"Error retrieving instantaneous values: {str(e)}"
            )

    def get_discharge_measurements(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get discharge measurements from the waterdata service.

        Args:
            params (Dict[str, Any]): Parameters containing sites and optionally
                                     start and end dates

        Returns:
            Dict[str, Any]: Response with discharge measurements
        """
        sites = params.get("sites")
        # split into list if comma-separated string
        if isinstance(sites, str):
            sites = sites.split(",")
        if not sites:
            return self._format_response("error", message="Sites parameter is required")

        start = params.get("start")
        end = params.get("end")

        try:
            df, md = nwis.get_discharge_measurements(sites=sites, start=start, end=end)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Retrieved {len(data_values)} discharge measurements"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def get_discharge_peaks(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get discharge peaks from the waterdata service.

        Args:
            params (Dict[str, Any]): Parameters containing sites and optionally
                                     start and end dates

        Returns:
            Dict[str, Any]: Response with discharge peaks
        """
        sites = params.get("sites")
        # split into list if comma-separated string
        if isinstance(sites, str):
            sites = sites.split(",")
        if not sites:
            return self._format_response("error", message="Sites parameter is required")

        start = params.get("start")
        end = params.get("end")

        try:
            df, md = nwis.get_discharge_peaks(sites=sites, start=start, end=end)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Retrieved {len(data_values)} discharge peaks"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def get_gwlevels(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get groundwater levels from the waterdata service.

        Args:
            params (Dict[str, Any]): Parameters containing sites and optionally
                                     start and end dates

        Returns:
            Dict[str, Any]: Response with groundwater levels
        """
        sites = params.get("sites")
        # split into list if comma-separated string
        if isinstance(sites, str):
            sites = sites.split(",")
        if not sites:
            return self._format_response("error", message="Sites parameter is required")

        start = params.get("start")
        end = params.get("end")

        try:
            df, md = nwis.get_gwlevels(sites=sites, start=start, end=end)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Retrieved {len(data_values)} groundwater level records"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def get_info(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get site description information from NWIS.

        Args:
            params (Dict[str, Any]): Parameters for the info query

        Returns:
            Dict[str, Any]: Response with site information
        """
        sites = params.get("sites")
        # split into list if comma-separated string
        if isinstance(sites, str):
            sites = sites.split(",")
        stateCd = params.get("stateCd")
        huc = params.get("huc")
        bBox = params.get("bBox")
        countyCd = params.get("countyCd")
        startDt = params.get("startDt")
        endDt = params.get("endDt")
        period = params.get("period")
        modifiedSince = params.get("modifiedSince")
        parameterCd = params.get("parameterCd")
        siteType = params.get("siteType")
        siteOutput = params.get("siteOutput")
        seriesCatalogOutput = params.get("seriesCatalogOutput")
        # at least one of the parameters is required
        if not any([sites, stateCd, huc, bBox, countyCd, startDt, endDt, period, modifiedSince, parameterCd, siteType]):
            return self._format_response("error", message="At least one of the parameters is required")
        try:
            df, md = nwis.get_info(**params)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Retrieved site information"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def get_pmcodes(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get NWIS parameter codes.

        Args:
            params (Dict[str, Any]): Parameters for the parameter code query

        Returns:
            Dict[str, Any]: Response with parameter codes
        """
        parameterCd = params.get("parameterCd")
        if not parameterCd:
            return self._format_response("error", message="Parameter code is required")
        try:
            df, md = nwis.get_pmcodes(parameterCd=parameterCd)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Retrieved {len(data_values,)} parameter codes"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def get_ratings(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get rating table for an active USGS streamgage.

        Args:
            params (Dict[str, Any]): Parameters containing site and optionally file_type

        Returns:
            Dict[str, Any]: Response with rating data
        """
        site = params.get("site")
        if not site:
            return self._format_response("error", message="Site parameter is required")

        file_type = params.get("file_type", "base")

        try:
            df, md = nwis.get_ratings(site=site, file_type=file_type)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Retrieved {len(data_values)} rating records for site {site}"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def get_record(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get data from NWIS.

        Args:
            params (Dict[str, Any]): Parameters for the record query

        Returns:
            Dict[str, Any]: Response with record data
        """
        try:
            df = nwis.get_record(**params)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Retrieved {len(data_values)} records"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def get_stats(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get water services statistics information.

        Args:
            params (Dict[str, Any]): Parameters for the statistics query

        Returns:
            Dict[str, Any]: Response with statistics data
        """
        sites = params.get("sites")
        # split into list if comma-separated string
        if isinstance(sites, str):
            sites = sites.split(",")
        parameterCd = params.get("parameterCd")
        statReportType = params.get("statReportType")
        statTypeCd = params.get("statTypeCd")

        try:
            df, md = nwis.get_stats(**params)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Retrieved statistical data"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def get_water_use(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get water use data from USGS (NWIS).

        Args:
            params (Dict[str, Any]): Parameters for the water use query

        Returns:
            Dict[str, Any]: Response with water use data
        """
        years = params.get("years")
        # make list if comma-separated string
        if years and isinstance(years, str):
            years = years.split(",")
        state = params.get("state")
        counties = params.get("counties")
        categories = params.get("categories")
        # must have at least one of the parameters
        if not any([years, state, counties, categories]):
            return self._format_response("error", message="At least one of the parameters is required")
        try:
            df, md = nwis.get_water_use(**params)
            # drop some extra columns
            df.drop(['state_cd', 'county_cd'], axis=1, errors='ignore', inplace=True)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            # column names and values separately
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Retrieved water use data"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def what_sites(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search NWIS for sites within a region with specific data.

        Args:
            params (Dict[str, Any]): Parameters for the site search

        Returns:
            Dict[str, Any]: Response with matching sites
        """
        try:
            df, md = nwis.what_sites(**params)
            # drop columns with all NaN values
            df.dropna(axis=1, how='all', inplace=True)
            # drop columns with all '-' values
            df = df.loc[:, (df != '-').any(axis=0)]
            column_names = df.columns.tolist()
            data_values = df.values.tolist()

            return self._format_response(
                "success",
                column_names=column_names,
                data=data_values,
                message=f"Found {len(data_values)} sites matching the criteria"
            )
        except Exception as e:
            return self._format_response("error", message=str(e))

    def format_mcp_context(self,
                          messages: Optional[List[Dict[str, Any]]] = None,
                          retrieval_results: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """
        Format context following the Model Context Protocol.

        Args:
            messages (Optional[List[Dict[str, Any]]], optional): List of conversation messages
            retrieval_results (Optional[List[Dict[str, Any]]], optional): List of retrieval results

        Returns:
            Dict[str, Any]: MCP-formatted context dictionary
        """
        context = {
            "functions": self.get_mcp_functions(),
            "metadata": {
                "source": "USGS Water Data",
                "description": "Data retrieval interface for USGS water data through dataretrieval library",
                "version": "1.0.0",
                "documentation_url": "https://doi-usgs.github.io/dataretrieval/index.html"
            }
        }

        if messages:
            context["messages"] = messages

        if retrieval_results:
            context["retrievals"] = retrieval_results

        return context

    def call_function(self, function_name: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Call a specific function by name with the given parameters.

        Args:
            function_name (str): Name of the function to call
            params (Dict[str, Any]): Parameters to pass to the function

        Returns:
            Dict[str, Any]: Response from the function
        """
        if function_name in self.functions:
            return self.functions[function_name](params)
        else:
            return self._format_response(
                "error",
                message=f"Function '{function_name}' not found. Available functions: {', '.join(self.functions.keys())}"
            )


# Example usage
if __name__ == "__main__":
    mcp_wrapper = MCPDataRetrieval()

    # Get MCP context
    context = mcp_wrapper.format_mcp_context()

    # Example function calls
    # get_site_data
    result = mcp_wrapper.call_function("get_site_data", {"site_code": "09380000"})
    print('site_data: ' + result['status'])

    # get_daily_values
    result = mcp_wrapper.call_function("get_daily_values", {"site_code": "09380000", "parameter_code": "00060", "statCd": "00003", "start_date": "2021-01-01", "end_date": "2021-01-10"})
    print('daily_values: ' + result['status'])

    # get_instantaneous_values
    result = mcp_wrapper.call_function("get_instantaneous_values", {"site_code": "09380000", "parameter_code": "00060", "start_date": "2024-01-01", "end_date": "2024-01-05"})
    print('instantaneous_values: ' + result['status'])

    # get_discharge_measurements
    result = mcp_wrapper.call_function("get_discharge_measurements", {"sites": "09380000"})
    print('discharge_measurements: ' + result['status'])

    # get_discharge_peaks
    result = mcp_wrapper.call_function("get_discharge_peaks", {"sites": "01594440", "start": "2020-01-01", "end": "2020-12-31"})
    print('discharge_peaks: ' + result['status'])

    # get_gwlevels
    result = mcp_wrapper.call_function("get_gwlevels", {"sites": "434400121275801"})
    print('gwlevels: ' + result['status'])

    # get_info
    result = mcp_wrapper.call_function("get_info", {"sites": "09380000"})
    print('info: ' + result['status'])

    # get_pmcodes
    result = mcp_wrapper.call_function("get_pmcodes", {"parameterCd": "00060"})
    print('pmcodes: ' + result['status'])

    # get_ratings
    result = mcp_wrapper.call_function("get_ratings", {"site": "09380000"})
    print('ratings: ' + result['status'])

    # get_record
    result = mcp_wrapper.call_function("get_record", {"sites": "09380000", "service": "site"})
    print('record: ' + result['status'])

    # get_stats
    result = mcp_wrapper.call_function("get_stats", {"sites": "09380000", "parameterCd": "00060", "statReportType": "daily", "statTypeCd": "mean"})
    print('stats: ' + result['status'])

    # get_water_use
    result = mcp_wrapper.call_function("get_water_use", {"years": "2015, 2020", "state": "PA"})
    print('water_use: ' + result['status'])

    # what_sites
    result = mcp_wrapper.call_function("what_sites", {"stateCd": "DE"})
    print('what_sites: ' + result['status'])