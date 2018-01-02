/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.exportclient;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltdb.export.AdvertisedDataSource;
import org.voltdb.exportclient.HttpExportClient.HttpMethod;

import com.google_voltpatches.common.collect.Lists;

public class TeradataListenerExportClient extends ExportClientBase {

	private static String CONTENT_TYPE = "Content-Type";
	private static String AUTHORIZATION = "Authorization";

	private static final VoltLogger logger = new VoltLogger("ExportClient");

	private static String m_endpoint = null;
	private static String content_type = null;
	private static String auth_token = null;
	private static Boolean m_batchMode = Boolean.FALSE;

	private static final int HTTP_EXPORT_MAX_CONNS = Integer.getInteger(
			"HTTP_EXPORT_MAX_CONNS", 20);

	private PoolingNHttpClientConnectionManager m_connManager = null;
	CloseableHttpAsyncClient m_client = HttpAsyncClients.createDefault();

	public TeradataListenerExportClient() {
		logger.info("Creating Teradata export client.");
	}

	@Override
	public void configure(Properties config) throws Exception {
		m_endpoint = config.getProperty("endpoint","").trim();
		if (m_endpoint.isEmpty()) {
			throw new IllegalArgumentException("HttpExportClient: must provide an endpoint");
		}
		content_type = config.getProperty("Content-Type", "").trim();
		auth_token = config.getProperty("Authorization", "").trim();
		m_batchMode = Boolean.parseBoolean(config.getProperty("batch.mode","true"));
	}

	private void connect() throws IOReactorException {
		if (m_connManager == null) {
			ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
			m_connManager = new PoolingNHttpClientConnectionManager(ioReactor);
			m_connManager.setMaxTotal(HTTP_EXPORT_MAX_CONNS);
			m_connManager.setDefaultMaxPerRoute(HTTP_EXPORT_MAX_CONNS);
		}

		if (m_client == null || !m_client.isRunning()) {
			HttpAsyncClientBuilder client = HttpAsyncClients.custom().setConnectionManager(m_connManager);
			m_client = client.build();
			m_client.start();
		}
	}

	public void shutdown() {
		try {
			if(m_client != null)
				m_client.close();
			if(m_connManager != null)
				m_connManager.shutdown(60 * 1000);
		} catch (IOException e) {
			logger.error("Error closing the HTTP client", e);
		}
	}

	@Override
	public ExportDecoderBase constructExportDecoder(AdvertisedDataSource source) {
		logger.debug("constructExportDecoder");
		return new TeradataExportDecoder(source);
	}

	class TeradataExportDecoder extends ExportDecoderBase {

		private JSONArray batchRequest = null;
		private final List<Future<HttpResponse>> m_outstanding = Lists.newArrayList();
		HttpMethod m_method = HttpMethod.POST;
		ContentType m_contentType = ContentType.APPLICATION_JSON;

		TeradataExportDecoder(AdvertisedDataSource source) {
			super(source);
		}

		@Override
		public void sourceNoLongerAdvertised(AdvertisedDataSource source) {}

		@Override
		public void onBlockStart() throws RestartBlockException {
			if(m_batchMode)
				batchRequest = new JSONArray();
			else
				m_outstanding.clear();
		}

		private JSONObject makeJSONObject(ExportRowData pojo) throws RestartBlockException {
			JSONObject jsonObject = new JSONObject();
			try {
				Object[] values = pojo.values;
				for(int i=0; i<values.length; i++) {
					jsonObject.put(this.m_source.columnName(i), values[i]);
				}

			} catch (JSONException e) {
				logger.error("Unable to create JSON Object", e);
				throw new RestartBlockException("Unable to create JSON Object",e,true);
			}
			return jsonObject;
		}

		@Override
		public boolean processRow(int rowSize, byte[] rowData) throws RestartBlockException {
			try {
				if (m_client == null || !m_client.isRunning()) {
					try {
						connect();
					} catch (IOReactorException e) {
						logger.error("Unable to create HTTP client", e);
						throw new RestartBlockException("Unable to create HTTP client",e,true);
					}
				}

				ExportRowData pojo = this.decodeRow(rowData);

				if(m_batchMode) {
					JSONObject obj = makeJSONObject(pojo);
					batchRequest.put(obj);
				} else {
					URI uri = new URI(m_endpoint);
					String reqBody = makeJSONObject(pojo).toString();
					HttpUriRequest request = makeRequest(uri, reqBody.toString());

					m_client.start();
					Future<HttpResponse> future = m_client.execute(request, new DebugCallback());
					m_outstanding.add(future);
				}
			} catch (IOException e) {
				throw new RestartBlockException(true);
			} catch (URISyntaxException e) {
				throw new RestartBlockException(true);
			}
			return true;  // see ENG-5116
		}

		private HttpUriRequest makeRequest(URI uri, final String requestBody)
		{
			HttpUriRequest request;
			if (m_method == HttpMethod.POST) {
				final HttpPost post = new HttpPost(uri);
				post.setEntity(new StringEntity(requestBody, m_contentType));
				request = post;
			} else {
				// Should never reach here
				request = null;
			}
			request.addHeader(CONTENT_TYPE, content_type);
			request.addHeader(AUTHORIZATION, auth_token);
			return request;
		}

		@Override
		public void onBlockCompletion() throws RestartBlockException {
			if(m_batchMode) {
				String reqBody = batchRequest.toString();
				try {
					HttpUriRequest request = makeRequest(new URI(m_endpoint), reqBody.toString());
					HttpResponse response = m_client.execute(request, null).get();
					if (checkResponse(response) == Boolean.FALSE) {
						throw new RestartBlockException("requeing on failed response check", true);
					}
				} catch (URISyntaxException e) {
					logger.error("Failure reported in connecting to endpoint " + m_endpoint);
					throw new RestartBlockException("Failure reported in connecting to endpoint " + m_endpoint, true);
				} catch (InterruptedException | ExecutionException e) {
					logger.error("Failure reported in request response.", e);
					throw new RestartBlockException("Failure reported in request response.", true);
				}
			} else {
				for (Future<HttpResponse> request : m_outstanding) {
					try {
						if (checkResponse(request.get()) == Boolean.FALSE) {
							throw new RestartBlockException("requeing on failed response check", true);
						}
					} catch (Exception e) {
						logger.error("Failure reported in request response.", e);
						throw new RestartBlockException("Failure reported in request response.", true);
					}
				}
			}
		}

		private Boolean checkResponse(HttpResponse response) {
			switch (response.getStatusLine().getStatusCode()) {
			case HttpStatus.SC_OK:
			case HttpStatus.SC_CREATED:
			case HttpStatus.SC_ACCEPTED:
				return Boolean.TRUE;
			case HttpStatus.SC_BAD_REQUEST:
			case HttpStatus.SC_NOT_FOUND:
			case HttpStatus.SC_FORBIDDEN:
				return Boolean.FALSE;
			default:
				return Boolean.FALSE;
			}
		}
	}

	private class DebugCallback implements FutureCallback<HttpResponse> {
		@Override
		public void cancelled() {
			logger.debug("Request cancelled");
		}

		@Override
		public void completed(HttpResponse arg0) {
			logger.debug("Request completed");
		}

		@Override
		public void failed(Exception arg0) {
			logger.debug("Request failed");
			arg0.printStackTrace();
		}
	}
}
