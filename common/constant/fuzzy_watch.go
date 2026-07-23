/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package constant

// FuzzyWatch protocol constants for the Naming module. These are wire
// strings the Nacos server matches literally, so they are copied verbatim
// from the Java source (alibaba/nacos, develop branch) rather than
// reinvented. Do not change any value here without re-verifying against
// the upstream Java constant it cites - client/server compatibility is
// negotiated purely on these strings.
const (
	// FUZZY_WATCH_PATTERN_SPLITTER joins fixNamespace/groupPattern/resourcePattern
	// into a groupKeyPattern, in that order.
	// Source: com.alibaba.nacos.api.common.Constants.FUZZY_WATCH_PATTERN_SPLITTER
	// https://github.com/alibaba/nacos/blob/develop/api/src/main/java/com/alibaba/nacos/api/common/Constants.java#L287
	// Concatenation order verified in FuzzyGroupKeyPattern.generatePattern:
	// https://github.com/alibaba/nacos/blob/develop/common/src/main/java/com/alibaba/nacos/common/utils/FuzzyGroupKeyPattern.java#L47-L64
	FUZZY_WATCH_PATTERN_SPLITTER = ">>"

	// FUZZY_WATCH_TYPE_WATCH / FUZZY_WATCH_TYPE_CANCEL_WATCH are the
	// NamingFuzzyWatchRequest.watchType values.
	// Source: Constants.WATCH_TYPE_WATCH / Constants.WATCH_TYPE_CANCEL_WATCH
	// https://github.com/alibaba/nacos/blob/develop/api/src/main/java/com/alibaba/nacos/api/common/Constants.java#L308-L317
	FUZZY_WATCH_TYPE_WATCH        = "WATCH"
	FUZZY_WATCH_TYPE_CANCEL_WATCH = "CANCEL_WATCH"

	// FUZZY_WATCH_INIT_NOTIFY / FUZZY_WATCH_DIFF_SYNC_NOTIFY /
	// FINISH_FUZZY_WATCH_INIT_NOTIFY are NamingFuzzyWatchSyncRequest.syncType
	// values the server sends while syncing a newly (re)registered pattern:
	// one INIT_NOTIFY per matched service during the initial batch(es),
	// DIFF_SYNC_NOTIFY for later incremental corrections, and a final
	// FINISH_FUZZY_WATCH_INIT_NOTIFY marking the initial sync complete.
	// Source: Constants.FUZZY_WATCH_INIT_NOTIFY / FUZZY_WATCH_DIFF_SYNC_NOTIFY /
	// FINISH_FUZZY_WATCH_INIT_NOTIFY
	// https://github.com/alibaba/nacos/blob/develop/api/src/main/java/com/alibaba/nacos/api/common/Constants.java#L289-L302
	FUZZY_WATCH_INIT_NOTIFY        = "FUZZY_WATCH_INIT_NOTIFY"
	FUZZY_WATCH_DIFF_SYNC_NOTIFY   = "FUZZY_WATCH_DIFF_SYNC_NOTIFY"
	FINISH_FUZZY_WATCH_INIT_NOTIFY = "FINISH_FUZZY_WATCH_INIT_NOTIFY"

	// FUZZY_WATCH_RESOURCE_CHANGED is the syncType used by
	// NamingFuzzyWatchChangeNotifyRequest - the server pushes this request
	// (distinct from NamingFuzzyWatchSyncRequest) whenever a single service
	// covered by an existing pattern is added or deleted after the initial
	// sync completed.
	// Source: Constants.FUZZY_WATCH_RESOURCE_CHANGED
	// https://github.com/alibaba/nacos/blob/develop/api/src/main/java/com/alibaba/nacos/api/common/Constants.java#L304-L307
	// Confirmed as the constructor arg for NamingFuzzyWatchChangeNotifyRequest:
	// https://github.com/alibaba/nacos/blob/develop/api/src/main/java/com/alibaba/nacos/api/naming/remote/request/NamingFuzzyWatchChangeNotifyRequest.java#L20-L34
	FUZZY_WATCH_RESOURCE_CHANGED = "FUZZY_WATCH_RESOURCE_CHANGED"

	// FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE / FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE
	// are the changedType values on NamingFuzzyWatchSyncRequest.Context and
	// NamingFuzzyWatchChangeNotifyRequest.
	// Source: Constants.ServiceChangedType.ADD_SERVICE / DELETE_SERVICE
	// https://github.com/alibaba/nacos/blob/develop/api/src/main/java/com/alibaba/nacos/api/common/Constants.java#L330-L341
	FUZZY_WATCH_CHANGED_TYPE_ADD_SERVICE    = "ADD_SERVICE"
	FUZZY_WATCH_CHANGED_TYPE_DELETE_SERVICE = "DELETE_SERVICE"

	// serviceKey (Context.serviceKey / NamingFuzzyWatchChangeNotifyRequest.serviceKey)
	// is namespace + SERVICE_INFO_SPLITER + group + SERVICE_INFO_SPLITER + serviceName;
	// SERVICE_INFO_SPLITER ("@@", already declared in const.go) is reused here
	// rather than redeclared.
	// Source: NamingUtils.getServiceKey(namespace, group, serviceName)
	// https://github.com/alibaba/nacos/blob/develop/api/src/main/java/com/alibaba/nacos/api/naming/utils/NamingUtils.java#L73-L79
	// Constants.SERVICE_INFO_SPLITER = "@@":
	// https://github.com/alibaba/nacos/blob/develop/api/src/main/java/com/alibaba/nacos/api/common/Constants.java#L197
)
