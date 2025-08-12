__all__ = ["Foxx"]

from typing import Any, Optional

from arangoasync.exceptions import (
    FoxxCommitError,
    FoxxConfigGetError,
    FoxxConfigReplaceError,
    FoxxConfigUpdateError,
    FoxxDependencyGetError,
    FoxxDependencyReplaceError,
    FoxxDependencyUpdateError,
    FoxxDevModeDisableError,
    FoxxDevModeEnableError,
    FoxxDownloadError,
    FoxxReadmeGetError,
    FoxxScriptListError,
    FoxxScriptRunError,
    FoxxServiceCreateError,
    FoxxServiceDeleteError,
    FoxxServiceGetError,
    FoxxServiceListError,
    FoxxServiceReplaceError,
    FoxxServiceUpdateError,
    FoxxSwaggerGetError,
    FoxxTestRunError,
)
from arangoasync.executor import ApiExecutor
from arangoasync.request import Method, Request
from arangoasync.response import Response
from arangoasync.result import Result
from arangoasync.serialization import Deserializer, Serializer
from arangoasync.typings import Json, Jsons, Params, RequestHeaders


class Foxx:
    """Foxx API wrapper."""

    def __init__(self, executor: ApiExecutor) -> None:
        self._executor = executor

    def __repr__(self) -> str:
        return f"<Foxx in {self._executor.db_name}>"

    @property
    def serializer(self) -> Serializer[Json]:
        """Return the serializer."""
        return self._executor.serializer

    @property
    def deserializer(self) -> Deserializer[Json, Jsons]:
        """Return the deserializer."""
        return self._executor.deserializer

    async def services(self, exclude_system: Optional[bool] = False) -> Result[Jsons]:
        """List installed services.

        Args:
            exclude_system (bool | None): Exclude system services.

        Returns:
            list: List of installed services.

        Raises:
            FoxxServiceListError: If retrieval fails.

        References:
            - `list-the-installed-services <https://docs.arangodb.com/stable/develop/http-api/foxx/#list-the-installed-services>`__
        """  # noqa: E501
        params: Params = {}
        if exclude_system is not None:
            params["excludeSystem"] = exclude_system

        request = Request(
            method=Method.GET,
            endpoint="/_api/foxx",
            params=params,
        )

        def response_handler(resp: Response) -> Jsons:
            if not resp.is_success:
                raise FoxxServiceListError(resp, request)
            result: Jsons = self.deserializer.loads_many(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def service(self, mount: str) -> Result[Json]:
        """Return service metadata.

        Args:
            mount (str): Service mount path (e.g "/_admin/aardvark").

        Returns:
            dict: Service metadata.

        Raises:
            FoxxServiceGetError: If retrieval fails.

        References:
            - `get-the-service-description <https://docs.arangodb.com/stable/develop/http-api/foxx/#get-the-service-description>`__
        """  # noqa: E501
        params: Params = {"mount": mount}
        request = Request(
            method=Method.GET,
            endpoint="/_api/foxx/service",
            params=params,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxServiceGetError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def create_service(
        self,
        mount: str,
        service: Any,
        headers: Optional[RequestHeaders] = None,
        development: Optional[bool] = None,
        setup: Optional[bool] = None,
        legacy: Optional[bool] = None,
    ) -> Result[Json]:
        """Installs the given new service at the given mount path.

        Args:
            mount (str): Mount path the service should be installed at.
            service (Any): Service payload. Can be a JSON string, a file-like object, or a
                multipart form.
            headers (dict | None): Request headers.
            development (bool | None): Whether to install the service in development mode.
            setup (bool | None): Whether to run the service setup script.
            legacy (bool | None): Whether to install in legacy mode.

        Returns:
            dict: Service metadata.

        Raises:
            FoxxServiceCreateError: If installation fails.

        References:
           - `install-a-new-service-mode <https://docs.arangodb.com/stable/develop/http-api/foxx/#install-a-new-service>`__
        """  # noqa: E501
        params: Params = dict()
        params["mount"] = mount
        if development is not None:
            params["development"] = development
        if setup is not None:
            params["setup"] = setup
        if legacy is not None:
            params["legacy"] = legacy

        if isinstance(service, dict):
            data = self.serializer.dumps(service)
        else:
            data = service

        request = Request(
            method=Method.POST,
            endpoint="/_api/foxx",
            params=params,
            data=data,
            headers=headers,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxServiceCreateError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def delete_service(
        self,
        mount: str,
        teardown: Optional[bool] = None,
    ) -> None:
        """Removes the service at the given mount path from the database and file system.

        Args:
            mount (str): Mount path of the service to uninstall.
            teardown (bool | None): Whether to run the teardown script.

        Raises:
            FoxxServiceDeleteError: If operations fails.

        References:
            - `uninstall-a-service <https://docs.arangodb.com/stable/develop/http-api/foxx/#uninstall-a-service>`__
        """  # noqa: E501
        params: Params = dict()
        params["mount"] = mount
        if teardown is not None:
            params["teardown"] = teardown

        request = Request(
            method=Method.DELETE,
            endpoint="/_api/foxx/service",
            params=params,
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise FoxxServiceDeleteError(resp, request)

        await self._executor.execute(request, response_handler)

    async def replace_service(
        self,
        mount: str,
        service: Any,
        headers: Optional[RequestHeaders] = None,
        teardown: Optional[bool] = None,
        setup: Optional[bool] = None,
        legacy: Optional[bool] = None,
        force: Optional[bool] = None,
    ) -> Result[Json]:
        """Replace an existing Foxx service at the given mount path.

        Args:
            mount (str): Mount path of the service to replace.
            service (Any): Service payload (JSON string, file-like object, or multipart form).
            headers (dict | None): Optional request headers.
            teardown (bool | None): Whether to run the teardown script.
            setup (bool | None): Whether to run the setup script.
            legacy (bool | None): Whether to install in legacy mode.
            force (bool | None): Set to `True` to force service install even if no service is installed under given mount.

        Returns:
            dict: Service metadata.

        Raises:
            FoxxServiceReplaceError: If replacement fails.

        References:
            - `replace-a-service <https://docs.arangodb.com/stable/develop/http-api/foxx/#replace-a-service>`__
        """  # noqa: E501
        params: Params = dict()
        params["mount"] = mount
        if teardown is not None:
            params["teardown"] = teardown
        if setup is not None:
            params["setup"] = setup
        if legacy is not None:
            params["legacy"] = legacy
        if force is not None:
            params["force"] = force

        if isinstance(service, dict):
            data = self.serializer.dumps(service)
        else:
            data = service

        request = Request(
            method=Method.PUT,
            endpoint="/_api/foxx/service",
            params=params,
            data=data,
            headers=headers,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxServiceReplaceError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def update_service(
        self,
        mount: str,
        service: Any,
        headers: Optional[RequestHeaders] = None,
        teardown: Optional[bool] = None,
        setup: Optional[bool] = None,
        legacy: Optional[bool] = None,
        force: Optional[bool] = None,
    ) -> Result[Json]:
        """Upgrade a Foxx service at the given mount path.

        Args:
            mount (str): Mount path of the service to upgrade.
            service (Any): Service payload (JSON string, file-like object, or multipart form).
            headers (dict | None): Optional request headers.
            teardown (bool | None): Whether to run the teardown script.
            setup (bool | None): Whether to run the setup script.
            legacy (bool | None): Whether to upgrade in legacy mode.
            force (bool | None): Set to `True` to force service install even if no service is installed under given mount.

        Returns:
            dict: Service metadata.

        Raises:
            FoxxServiceUpdateError: If upgrade fails.

        References:
            - `upgrade-a-service <https://docs.arangodb.com/stable/develop/http-api/foxx/#upgrade-a-service>`__
        """  # noqa: E501
        params: Params = dict()
        params["mount"] = mount
        if teardown is not None:
            params["teardown"] = teardown
        if setup is not None:
            params["setup"] = setup
        if legacy is not None:
            params["legacy"] = legacy
        if force is not None:
            params["force"] = force

        if isinstance(service, dict):
            data = self.serializer.dumps(service)
        else:
            data = service

        request = Request(
            method=Method.PATCH,
            endpoint="/_api/foxx/service",
            params=params,
            data=data,
            headers=headers,
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxServiceUpdateError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def config(self, mount: str) -> Result[Json]:
        """Return service configuration.

        Args:
            mount (str): Service mount path.

        Returns:
            dict: Service configuration.

        Raises:
            FoxxConfigGetError: If retrieval fails.

        References:
            - `get-the-configuration-options <https://docs.arangodb.com/stable/develop/http-api/foxx/#get-the-configuration-options>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/foxx/configuration",
            params={"mount": mount},
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxConfigGetError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def update_config(self, mount: str, options: Json) -> Result[Json]:
        """Update service configuration.

        Args:
            mount (str): Service mount path.
            options (dict): Configuration values. Omitted options are ignored.

        Returns:
            dict: Updated configuration values.

        Raises:
            FoxxConfigUpdateError: If update fails.

        References:
            - `update-the-configuration-options <https://docs.arangodb.com/stable/develop/http-api/foxx/#update-the-configuration-options>`__
        """  # noqa: E501
        request = Request(
            method=Method.PATCH,
            endpoint="/_api/foxx/configuration",
            params={"mount": mount},
            data=self.serializer.dumps(options),
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxConfigUpdateError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def replace_config(self, mount: str, options: Json) -> Result[Json]:
        """Replace service configuration.

        Args:
            mount (str): Service mount path.
            options (dict): Configuration values. Omitted options are reset to their
                default values or marked as un-configured.

        Returns:
            dict: Replaced configuration values.

        Raises:
            FoxxConfigReplaceError: If replace fails.

        References:
            - `replace-the-configuration-options <https://docs.arangodb.com/stable/develop/http-api/foxx/#replace-the-configuration-options>`__
        """  # noqa: E501
        request = Request(
            method=Method.PUT,
            endpoint="/_api/foxx/configuration",
            params={"mount": mount},
            data=self.serializer.dumps(options),
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxConfigReplaceError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def dependencies(self, mount: str) -> Result[Json]:
        """Return service dependencies.

        Args:
            mount (str): Service mount path.

        Returns:
            dict: Service dependencies settings.

        Raises:
            FoxxDependencyGetError: If retrieval fails.

        References:
           - `get-the-dependency-options <https://docs.arangodb.com/stable/develop/http-api/foxx/#get-the-dependency-options>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/foxx/dependencies",
            params={"mount": mount},
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxDependencyGetError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def update_dependencies(self, mount: str, options: Json) -> Result[Json]:
        """Update service dependencies.

        Args:
            mount (str): Service mount path.
            options (dict): Dependencies settings. Omitted ones are ignored.

        Returns:
            dict: Updated dependency settings.

        Raises:
            FoxxDependencyUpdateError: If update fails.

        References:
            - `update-the-dependency-options <https://docs.arangodb.com/stable/develop/http-api/foxx/#update-the-dependency-options>`__
        """  # noqa: E501
        request = Request(
            method=Method.PATCH,
            endpoint="/_api/foxx/dependencies",
            params={"mount": mount},
            data=self.serializer.dumps(options),
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxDependencyUpdateError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def replace_dependencies(self, mount: str, options: Json) -> Result[Json]:
        """Replace service dependencies.

        Args:
            mount (str): Service mount path.
            options (dict): Dependencies settings. Omitted ones are disabled.

        Returns:
            dict: Replaced dependency settings.

        Raises:
            FoxxDependencyReplaceError: If replace fails.

        References:
            - `replace-the-dependency-options <https://docs.arangodb.com/stable/develop/http-api/foxx/#replace-the-dependency-options>`__
        """  # noqa: E501
        request = Request(
            method=Method.PUT,
            endpoint="/_api/foxx/dependencies",
            params={"mount": mount},
            data=self.serializer.dumps(options),
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxDependencyReplaceError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def scripts(self, mount: str) -> Result[Json]:
        """List service scripts.

        Args:
            mount (str): Service mount path.

        Returns:
            dict: Service scripts.

        Raises:
            FoxxScriptListError: If retrieval fails.

        References:
            - `list-the-service-scripts <https://docs.arangodb.com/stable/develop/http-api/foxx/#list-the-service-scripts>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/foxx/scripts",
            params={"mount": mount},
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxScriptListError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def run_script(
        self, mount: str, name: str, arg: Optional[Json] = None
    ) -> Result[Any]:
        """Run a service script.

        Args:
            mount (str): Service mount path.
            name (str): Script name.
            arg (dict | None): Arbitrary value passed into the script as first argument.

        Returns:
            Any: Returns the exports of the script, if any.

        Raises:
            FoxxScriptRunError: If script fails.

        References:
           - `run-a-service-script <https://docs.arangodb.com/stable/develop/http-api/foxx/#run-a-service-script>`__
        """  # noqa: E501
        request = Request(
            method=Method.POST,
            endpoint=f"/_api/foxx/scripts/{name}",
            params={"mount": mount},
            data=self.serializer.dumps(arg) if arg is not None else None,
        )

        def response_handler(resp: Response) -> Any:
            if not resp.is_success:
                raise FoxxScriptRunError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def run_tests(
        self,
        mount: str,
        reporter: Optional[str] = None,
        idiomatic: Optional[bool] = None,
        filter: Optional[str] = None,
        output_format: Optional[str] = None,
    ) -> Result[str]:
        """Run service tests.

        Args:
            mount (str): Service mount path.
            reporter (str | None): Test reporter. Allowed values are "default" (simple
                list of test cases), "suite" (object of test cases nested in
                suites), "stream" (raw stream of test results), "xunit" (XUnit or
                JUnit compatible structure), or "tap" (raw TAP compatible stream).
            idiomatic (bool | None): Use matching format for the reporter, regardless of
                the value of parameter **output_format**.
            filter (str | None): Only run tests whose full name (test suite and
                test case) matches the given string.
            output_format (str | None): Used to further control format. Allowed values
                are "x-ldjson", "xml" and "text". When using "stream" reporter,
                setting this to "x-ldjson" returns newline-delimited JSON stream.
                When using "tap" reporter, setting this to "text" returns plain
                text TAP report. When using "xunit" reporter, settings this to
                "xml" returns an XML instead of JSONML.

        Returns:
            str: Reporter output (e.g. raw JSON string, XML, plain text).

        Raises:
            FoxxTestRunError: If test fails.

        References:
           - `run-the-service-tests <https://docs.arangodb.com/stable/develop/http-api/foxx/#run-the-service-tests>`__
        """  # noqa: E501
        params: Params = dict()
        params["mount"] = mount
        if reporter is not None:
            params["reporter"] = reporter
        if idiomatic is not None:
            params["idiomatic"] = idiomatic
        if filter is not None:
            params["filter"] = filter

        headers: RequestHeaders = {}
        if output_format == "x-ldjson":
            headers["accept"] = "application/x-ldjson"
        elif output_format == "xml":
            headers["accept"] = "application/xml"
        elif output_format == "text":
            headers["accept"] = "text/plain"

        request = Request(
            method=Method.POST,
            endpoint="/_api/foxx/tests",
            params=params,
            headers=headers,
        )

        def response_handler(resp: Response) -> str:
            if not resp.is_success:
                raise FoxxTestRunError(resp, request)
            return resp.raw_body.decode("utf-8")

        return await self._executor.execute(request, response_handler)

    async def enable_development(self, mount: str) -> Result[Json]:
        """Puts the service into development mode.

        While the service is running in development mode, it is reloaded from
        the file system, and its setup script (if any) is re-executed every
        time the service handles a request.

        In a cluster with multiple coordinators, changes to the filesystem on
        one coordinator is not reflected across other coordinators.

        Args:
            mount (str): Service mount path.

        Returns:
            dict: Service metadata.

        Raises:
            FoxxDevModeEnableError: If the operation fails.

        References:
           - `enable-the-development-mode <https://docs.arangodb.com/stable/develop/http-api/foxx/#enable-the-development-mode>`__
        """  # noqa: E501
        request = Request(
            method=Method.POST,
            endpoint="/_api/foxx/development",
            params={"mount": mount},
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxDevModeEnableError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def disable_development(self, mount: str) -> Result[Json]:
        """Puts the service into production mode.

        In a cluster with multiple coordinators, the services on all other
        coordinators are replaced with the version on the calling coordinator.

        Args:
            mount (str): Service mount path.

        Returns:
            dict: Service metadata.

        Raises:
            FoxxDevModeDisableError: If the operation fails.

        References:
            - `disable-the-development-mode <https://docs.arangodb.com/stable/develop/http-api/foxx/#disable-the-development-mode>`__
        """  # noqa: E501
        request = Request(
            method=Method.DELETE,
            endpoint="/_api/foxx/development",
            params={"mount": mount},
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxDevModeDisableError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def readme(self, mount: str) -> Result[str]:
        """Return the service readme.

        Args:
            mount (str): Service mount path.

        Returns:
            str: Service readme content.

        Raises:
            FoxxReadmeGetError: If retrieval fails.

        References:
            - `get-the-service-readme <https://docs.arangodb.com/stable/develop/http-api/foxx/#get-the-service-readme>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET,
            endpoint="/_api/foxx/readme",
            params={"mount": mount},
        )

        def response_handler(resp: Response) -> str:
            if not resp.is_success:
                raise FoxxReadmeGetError(resp, request)
            return resp.raw_body.decode("utf-8")

        return await self._executor.execute(request, response_handler)

    async def swagger(self, mount: str) -> Result[Json]:
        """Return the Swagger API description for the given service.

        Args:
            mount (str): Service mount path.

        Returns:
            dict: Swagger API description.

        Raises:
            FoxxSwaggerGetError: If retrieval fails.

        References:
           - `get-the-swagger-description <https://docs.arangodb.com/stable/develop/http-api/foxx/#get-the-swagger-description>`__
        """  # noqa: E501
        request = Request(
            method=Method.GET, endpoint="/_api/foxx/swagger", params={"mount": mount}
        )

        def response_handler(resp: Response) -> Json:
            if not resp.is_success:
                raise FoxxSwaggerGetError(resp, request)
            result: Json = self.deserializer.loads(resp.raw_body)
            return result

        return await self._executor.execute(request, response_handler)

    async def download(self, mount: str) -> Result[bytes]:
        """Downloads a zip bundle of the service directory.

        When development mode is enabled, this always creates a new bundle.
        Otherwise, the bundle will represent the version of a service that is
        installed on that ArangoDB instance.

        Args:
            mount (str): Service mount path.

        Returns:
            bytes: Service bundle zip in raw bytes form.

        Raises:
            FoxxDownloadError: If download fails.

        References:
           - `download-a-service-bundle <https://docs.arangodb.com/stable/develop/http-api/foxx/#download-a-service-bundle>`__
        """  # noqa: E501
        request = Request(
            method=Method.POST, endpoint="/_api/foxx/download", params={"mount": mount}
        )

        def response_handler(resp: Response) -> bytes:
            if not resp.is_success:
                raise FoxxDownloadError(resp, request)
            return resp.raw_body

        return await self._executor.execute(request, response_handler)

    async def commit(self, replace: Optional[bool] = None) -> None:
        """Commit local service state of the coordinator to the database.

        This can be used to resolve service conflicts between coordinators
        that cannot be fixed automatically due to missing data.

        Args:
            replace (bool | None): If set to `True`, any existing service files in the database
                will be overwritten.

        Raises:
            FoxxCommitError: If commit fails.

        References:
            - `commit-the-local-service-state <https://docs.arangodb.com/stable/develop/http-api/foxx/#commit-the-local-service-state>`__
        """  # noqa: E501
        params: Params = {}
        if replace is not None:
            params["replace"] = replace

        request = Request(
            method=Method.POST, endpoint="/_api/foxx/commit", params=params
        )

        def response_handler(resp: Response) -> None:
            if not resp.is_success:
                raise FoxxCommitError(resp, request)

        await self._executor.execute(request, response_handler)
