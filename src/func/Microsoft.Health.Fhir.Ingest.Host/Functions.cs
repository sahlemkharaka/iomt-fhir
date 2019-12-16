// -------------------------------------------------------------------------------------------------
// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License (MIT). See LICENSE in the repo root for license information.
// -------------------------------------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using EnsureThat;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Health.Common.Telemetry;
using Microsoft.Health.Fhir.Ingest.Data;
using Microsoft.Health.Fhir.Ingest.Host;
using Microsoft.Health.Fhir.Ingest.Telemetry;
using Microsoft.Health.Fhir.Ingest.Template;

[assembly: System.Resources.NeutralResourcesLanguage("en")]

namespace Microsoft.Health.Fhir.Ingest.Service
{
    public static class Functions
    {
        [FunctionName("MeasurementCollectionToFhir")]
        public static async Task<IActionResult> MeasurementCollectionToFhir(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            [MeasurementFhirImport] MeasurementFhirImportService measurementImportService,
            ILogger log)
        {
            EnsureArg.IsNotNull(measurementImportService, nameof(measurementImportService));
            EnsureArg.IsNotNull(req, nameof(req));

            try
            {
                await measurementImportService.ProcessStreamAsync(req.Body, Templates.FhirMapping, log).ConfigureAwait(false);
                return new AcceptedResult();
            }
            catch (Exception ex)
            {
                log.RecordUnhandledExceptionMetrics(ex, nameof(MeasurementCollectionToFhir));
                throw;
            }
        }

        [FunctionName("NormalizeDeviceData")]
        public static async Task NormalizeDeviceData(
            [EventHubTrigger("input", Connection = "InputEventHub")] EventData[] events,
            [EventHubMeasurementCollector("output", Connection = "OutputEventHub")] IAsyncCollector<IMeasurement> output,
            ILogger log)
        {
            try
            {
                EnsureArg.IsNotNull(events, nameof(events));

                var template = CollectionContentTemplateFactory.Default.Create(Templates.DeviceContent);

                log.LogMetric(Metrics.DeviceEvent, events.Length);
                IDataNormalizationService<EventData, IMeasurement> dataNormalizationService = new MeasurementEventNormalizationService(log, template);
                await dataNormalizationService.ProcessAsync(events, output).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                log.RecordUnhandledExceptionMetrics(ex, nameof(NormalizeDeviceData));
                throw;
            }
        }
    }
}
