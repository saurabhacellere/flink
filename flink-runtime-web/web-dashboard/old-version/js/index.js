angular.module("flinkApp", ["ui.router", "angularMoment", "dndLists"]).run(["$rootScope", function (e) {
    return e.sidebarVisible = !1, e.showSidebar = function () {
        return e.sidebarVisible = !e.sidebarVisible, e.sidebarClass = "force-show"
    }
}]).value("flinkConfig", {
    jobServer: "../",
    "refresh-interval": 1e4
}).value("watermarksConfig", {noWatermark: -0x8000000000000000}).run(["JobsService", "MainService", "flinkConfig", "$interval", function (e, t, r, n) {
    return t.loadConfig().then(function (t) {
        return angular.extend(r, t), e.listJobs(), n(function () {
            return e.listJobs()
        }, r["refresh-interval"])
    })
}]).config(["$uiViewScrollProvider", function (e) {
    return e.useAnchorScroll()
}]).run(["$rootScope", "$state", function (e, t) {
    return e.$on("$stateChangeStart", function (e, r, n, o) {
        if (r.redirectTo) return e.preventDefault(), t.go(r.redirectTo, n)
    })
}]).config(["$stateProvider", "$urlRouterProvider", function (e, t) {
    return e.state("overview", {
        url: "/overview",
        views: {main: {templateUrl: "partials/overview.html", controller: "OverviewController"}}
    }).state("running-jobs", {
        url: "/running-jobs",
        views: {main: {templateUrl: "partials/jobs/running-jobs.html", controller: "RunningJobsController"}}
    }).state("completed-jobs", {
        url: "/completed-jobs",
        views: {main: {templateUrl: "partials/jobs/completed-jobs.html", controller: "CompletedJobsController"}}
    }).state("single-job", {
        url: "/jobs/{jobid}",
        "abstract": !0,
        views: {main: {templateUrl: "partials/jobs/job.html", controller: "SingleJobController"}}
    }).state("single-job.plan", {
        url: "",
        redirectTo: "single-job.plan.subtasks",
        views: {details: {templateUrl: "partials/jobs/job.plan.html", controller: "JobPlanController"}}
    }).state("single-job.plan.subtasks", {
        url: "",
        views: {
            "node-details": {
                templateUrl: "partials/jobs/job.plan.node-list.subtasks.html",
                controller: "JobPlanSubtasksController"
            }
        }
    }).state("single-job.plan.metrics", {
        url: "/metrics",
        views: {
            "node-details": {
                templateUrl: "partials/jobs/job.plan.node-list.metrics.html",
                controller: "JobPlanMetricsController"
            }
        }
    }).state("single-job.plan.watermarks", {
        url: "/watermarks",
        views: {"node-details": {templateUrl: "partials/jobs/job.plan.node-list.watermarks.html"}}
    }).state("single-job.plan.accumulators", {
        url: "/accumulators",
        views: {
            "node-details": {
                templateUrl: "partials/jobs/job.plan.node-list.accumulators.html",
                controller: "JobPlanAccumulatorsController"
            }
        }
    }).state("single-job.plan.checkpoints", {
        url: "/checkpoints",
        redirectTo: "single-job.plan.checkpoints.overview",
        views: {
            "node-details": {
                templateUrl: "partials/jobs/job.plan.node-list.checkpoints.html",
                controller: "JobPlanCheckpointsController"
            }
        }
    }).state("single-job.plan.checkpoints.overview", {
        url: "/overview",
        views: {
            "checkpoints-view": {
                templateUrl: "partials/jobs/job.plan.node.checkpoints.overview.html",
                controller: "JobPlanCheckpointsController"
            }
        }
    }).state("single-job.plan.checkpoints.summary", {
        url: "/summary",
        views: {
            "checkpoints-view": {
                templateUrl: "partials/jobs/job.plan.node.checkpoints.summary.html",
                controller: "JobPlanCheckpointsController"
            }
        }
    }).state("single-job.plan.checkpoints.history", {
        url: "/history",
        views: {
            "checkpoints-view": {
                templateUrl: "partials/jobs/job.plan.node.checkpoints.history.html",
                controller: "JobPlanCheckpointsController"
            }
        }
    }).state("single-job.plan.checkpoints.config", {
        url: "/config",
        views: {
            "checkpoints-view": {
                templateUrl: "partials/jobs/job.plan.node.checkpoints.config.html",
                controller: "JobPlanCheckpointsController"
            }
        }
    }).state("single-job.plan.checkpoints.details", {
        url: "/details/{checkpointId}",
        views: {
            "checkpoints-view": {
                templateUrl: "partials/jobs/job.plan.node.checkpoints.details.html",
                controller: "JobPlanCheckpointDetailsController"
            }
        }
    }).state("single-job.plan.backpressure", {
        url: "/backpressure",
        views: {
            "node-details": {
                templateUrl: "partials/jobs/job.plan.node-list.backpressure.html",
                controller: "JobPlanBackPressureController"
            }
        }
    }).state("single-job.timeline", {
        url: "/timeline",
        views: {details: {templateUrl: "partials/jobs/job.timeline.html"}}
    }).state("single-job.timeline.vertex", {
        url: "/{vertexId}",
        views: {
            vertex: {
                templateUrl: "partials/jobs/job.timeline.vertex.html",
                controller: "JobTimelineVertexController"
            }
        }
    }).state("single-job.exceptions", {
        url: "/exceptions",
        views: {details: {templateUrl: "partials/jobs/job.exceptions.html", controller: "JobExceptionsController"}}
    }).state("single-job.config", {
        url: "/config",
        views: {details: {templateUrl: "partials/jobs/job.config.html"}}
    }).state("all-manager", {
        url: "/taskmanagers",
        views: {main: {templateUrl: "partials/taskmanager/index.html", controller: "AllTaskManagersController"}}
    }).state("single-manager", {
        url: "/taskmanager/{taskmanagerid}",
        "abstract": !0,
        views: {main: {templateUrl: "partials/taskmanager/taskmanager.html", controller: "SingleTaskManagerController"}}
    }).state("single-manager.metrics", {
        url: "/metrics",
        views: {details: {templateUrl: "partials/taskmanager/taskmanager.metrics.html"}}
    }).state("single-manager.stdout", {
        url: "/stdout",
        views: {
            details: {
                templateUrl: "partials/taskmanager/taskmanager.stdout.html",
                controller: "SingleTaskManagerStdoutController"
            }
        }
    }).state("single-manager.dump", {
        url: "/dump",
        views: {
            details: {
                templateUrl: "partials/taskmanager/taskmanager.dump.html",
                controller: "SingleTaskManagerDumpController"
            }
        }
    }).state("single-manager.log", {
        url: "/log",
        views: {
            details: {
                templateUrl: "partials/taskmanager/taskmanager.log.html",
                controller: "SingleTaskManagerLogsController"
            }
        }
    }).state("jobmanager", {
        url: "/jobmanager",
        views: {main: {templateUrl: "partials/jobmanager/index.html"}}
    }).state("jobmanager.config", {
        url: "/config",
        views: {details: {templateUrl: "partials/jobmanager/config.html", controller: "JobManagerConfigController"}}
    }).state("jobmanager.stdout", {
        url: "/stdout",
        views: {details: {templateUrl: "partials/jobmanager/stdout.html", controller: "JobManagerStdoutController"}}
    }).state("jobmanager.log", {
        url: "/log",
        views: {details: {templateUrl: "partials/jobmanager/log.html", controller: "JobManagerLogsController"}}
    }).state("submit", {
        url: "/submit",
        views: {main: {templateUrl: "partials/submit.html", controller: "JobSubmitController"}}
    }), t.otherwise("/overview")
}]), angular.module("flinkApp").directive("bsLabel", ["JobsService", function (e) {
    return {
        transclude: !0,
        replace: !0,
        scope: {getLabelClass: "&", status: "@"},
        template: "<span title='{{status}}' ng-class='getLabelClass()'><ng-transclude></ng-transclude></span>",
        link: function (t, r, n) {
            return t.getLabelClass = function () {
                return "label label-" + e.translateLabelState(n.status)
            }
        }
    }
}]).directive("bpLabel", ["JobsService", function (e) {
    return {
        transclude: !0,
        replace: !0,
        scope: {getBackPressureLabelClass: "&", status: "@"},
        template: "<span title='{{status}}' ng-class='getBackPressureLabelClass()'><ng-transclude></ng-transclude></span>",
        link: function (t, r, n) {
            return t.getBackPressureLabelClass = function () {
                return "label label-" + e.translateBackPressureLabelState(n.status)
            }
        }
    }
}]).directive("indicatorPrimary", ["JobsService", function (e) {
    return {
        replace: !0,
        scope: {getLabelClass: "&", status: "@"},
        template: "<i title='{{status}}' ng-class='getLabelClass()' />",
        link: function (t, r, n) {
            return t.getLabelClass = function () {
                return "fa fa-circle indicator indicator-" + e.translateLabelState(n.status)
            }
        }
    }
}]).directive("tableProperty", function () {
    return {replace: !0, scope: {value: "="}, template: "<td title=\"{{value || 'None'}}\">{{value || 'None'}}</td>"}
}), angular.module("flinkApp").filter("amDurationFormatExtended", ["angularMomentConfig", function (e) {
    var t;
    return t = function (e, t, r) {
        return "undefined" == typeof e || null === e ? "" : moment.duration(e, t).format(r, {trim: !1})
    }, t.$stateful = e.statefulFilters, t
}]).filter("humanizeDuration", function () {
    return function (e, t) {
        var r, n, o, i, a, s;
        return "undefined" == typeof e || null === e ? "" : (i = e % 1e3, s = Math.floor(e / 1e3), a = s % 60, s = Math.floor(s / 60), o = s % 60, s = Math.floor(s / 60), n = s % 24, s = Math.floor(s / 24), r = s, 0 === r ? 0 === n ? 0 === o ? 0 === a ? i + "ms" : a + "s " : o + "m " + a + "s" : t ? n + "h " + o + "m" : n + "h " + o + "m " + a + "s" : t ? r + "d " + n + "h" : r + "d " + n + "h " + o + "m " + a + "s")
    }
}).filter("limit", function () {
    return function (e) {
        return e.length > 73 && (e = e.substring(0, 35) + "..." + e.substring(e.length - 35, e.length)), e
    }
}).filter("humanizeText", function () {
    return function (e) {
        return e ? e.replace(/&gt;/g, ">").replace(/<br\/>/g, "") : ""
    }
}).filter("humanizeBytes", function () {
    return function (e) {
        var t, r;
        return r = ["B", "KB", "MB", "GB", "TB", "PB", "EB"], t = function (e, n) {
            var o;
            return o = Math.pow(1024, n), e < o ? (e / o).toFixed(2) + " " + r[n] : e < 1e3 * o ? (e / o).toPrecision(3) + " " + r[n] : t(e, n + 1)
        }, "undefined" == typeof e || null === e ? "" : e < 1e3 ? e + " B" : t(e, 1)
    }
}).filter("toLocaleString", function () {
    return function (e) {
        return e.toLocaleString()
    }
}).filter("toUpperCase", function () {
    return function (e) {
        return e.toUpperCase()
    }
}).filter("percentage", function () {
    return function (e) {
        return (100 * e).toFixed(0) + "%"
    }
}).filter("humanizeWatermark", ["watermarksConfig", function (e) {
    return function (t) {
        return isNaN(t) || t <= e.noWatermark ? "No Watermark" : t
    }
}]).filter("increment", function () {
    return function (e) {
        return parseInt(e) + 1
    }
}).filter("humanizeChartNumeric", ["humanizeBytesFilter", "humanizeDurationFilter", function (e, t) {
    return function (r, n) {
        var o;
        return o = "", null !== r && (o = /bytes/i.test(n.id) && /persecond/i.test(n.id) ? e(r) + " / s" : /bytes/i.test(n.id) ? e(r) : /persecond/i.test(n.id) ? r + " / s" : /time/i.test(n.id) || /latency/i.test(n.id) ? t(r, !0) : r), o
    }
}]).filter("humanizeChartNumericTitle", ["humanizeDurationFilter", function (e) {
    return function (t, r) {
        var n;
        return n = "", null !== t && (n = /bytes/i.test(r.id) && /persecond/i.test(r.id) ? t + " Bytes / s" : /bytes/i.test(r.id) ? t + " Bytes" : /persecond/i.test(r.id) ? t + " / s" : /time/i.test(r.id) || /latency/i.test(r.id) ? e(t, !1) : t), n
    }
}]).filter("searchMetrics", function () {
    return function (e, t) {
        var r, n;
        return n = new RegExp(t, "gi"), function () {
            var t, o, i;
            for (i = [], t = 0, o = e.length; t < o; t++) r = e[t], r.id.match(n) && i.push(r);
            return i
        }()
    }
}), angular.module("flinkApp").service("MainService", ["$http", "flinkConfig", "$q", function (e, t, r) {
    return this.loadConfig = function () {
        var n;
        return n = r.defer(), e.get(t.jobServer + "config").success(function (e, t, r, o) {
            return n.resolve(e)
        }), n.promise
    }, this
}]), angular.module("flinkApp").controller("JobManagerConfigController", ["$scope", "JobManagerConfigService", function (e, t) {
    return t.loadConfig().then(function (t) {
        return null == e.jobmanager && (e.jobmanager = {}), e.jobmanager.config = t
    })
}]).controller("JobManagerLogsController", ["$scope", "JobManagerLogsService", function (e, t) {
    return t.loadLogs().then(function (t) {
        return null == e.jobmanager && (e.jobmanager = {}), e.jobmanager.log = t
    }), e.reloadData = function () {
        return t.loadLogs().then(function (t) {
            return e.jobmanager.log = t
        })
    }
}]).controller("JobManagerStdoutController", ["$scope", "JobManagerStdoutService", function (e, t) {
    return t.loadStdout().then(function (t) {
        return null == e.jobmanager && (e.jobmanager = {}), e.jobmanager.stdout = t
    }), e.reloadData = function () {
        return t.loadStdout().then(function (t) {
            return e.jobmanager.stdout = t
        })
    }
}]), angular.module("flinkApp").service("JobManagerConfigService", ["$http", "flinkConfig", "$q", function (e, t, r) {
    var n;
    return n = {}, this.loadConfig = function () {
        var n;
        return n = r.defer(), e.get(t.jobServer + "jobmanager/config").success(function (e, t, r, o) {
            return o = e, n.resolve(e)
        }), n.promise
    }, this
}]).service("JobManagerLogsService", ["$http", "flinkConfig", "$q", function (e, t, r) {
    var n;
    return n = {}, this.loadLogs = function () {
        var o;
        return o = r.defer(), e.get(t.jobServer + "jobmanager/log").success(function (e, t, r, i) {
            return n = e, o.resolve(e)
        }), o.promise
    }, this
}]).service("JobManagerStdoutService", ["$http", "flinkConfig", "$q", function (e, t, r) {
    var n;
    return n = {}, this.loadStdout = function () {
        var o;
        return o = r.defer(), e.get(t.jobServer + "jobmanager/stdout").success(function (e, t, r, i) {
            return n = e, o.resolve(e)
        }), o.promise
    }, this
}]), angular.module("flinkApp").controller("RunningJobsController", ["$scope", "$state", "$stateParams", "JobsService", function (e, t, r, n) {
    return e.jobObserver = function () {
        return e.jobs = n.getJobs("running")
    }, n.registerObserver(e.jobObserver), e.$on("$destroy", function () {
        return n.unRegisterObserver(e.jobObserver)
    }), e.jobObserver()
}]).controller("CompletedJobsController", ["$scope", "$state", "$stateParams", "JobsService", function (e, t, r, n) {
    return e.jobObserver = function () {
        return e.jobs = n.getJobs("finished")
    }, n.registerObserver(e.jobObserver), e.$on("$destroy", function () {
        return n.unRegisterObserver(e.jobObserver)
    }), e.jobObserver()
}]).controller("SingleJobController", ["$scope", "$state", "$stateParams", "JobsService", "MetricsService", "$rootScope", "flinkConfig", "$interval", "$q", "watermarksConfig", function (e, t, r, n, o, i, a, s, l, u) {
    var c, d;
    return e.jobid = r.jobid, e.job = null, e.plan = null, e.watermarks = {}, e.vertices = null, e.backPressureOperatorStats = {}, d = s(function () {
        return n.loadJob(r.jobid).then(function (t) {
            return e.job = t, e.$broadcast("reload")
        })
    }, a["refresh-interval"]), e.$on("$destroy", function () {
        return e.job = null, e.plan = null, e.watermarks = {}, e.vertices = null, e.backPressureOperatorStats = null, s.cancel(d)
    }), e.cancelJob = function (e) {
        return angular.element(e.currentTarget).removeClass("btn").removeClass("btn-default").html("Cancelling..."), n.cancelJob(r.jobid).then(function (e) {
            return {}
        })
    }, n.loadJob(r.jobid).then(function (t) {
        return e.job = t, e.vertices = t.vertices, e.plan = t.plan, o.setupMetrics(r.jobid, t.vertices)
    }), c = function (t) {
        var r, n, i, a;
        return i = function (t) {
            return function (t) {
                var r, n, i, a;
                return r = l.defer(), i = e.job.jid, a = function () {
                    var e, r, o;
                    for (o = [], n = e = 0, r = t.parallelism - 1; 0 <= r ? e <= r : e >= r; n = 0 <= r ? ++e : --e) o.push(n + ".currentInputWatermark");
                    return o
                }(), o.getMetrics(i, t.id, a).then(function (e) {
                    var t, n, o, i, a, s, l;
                    o = NaN, l = {}, i = e.values;
                    for (t in i) s = i[t], a = t.replace(".currentInputWatermark", ""), l[a] = s, (isNaN(o) || s < o) && (o = s);
                    return n = !isNaN(o) && o > u.noWatermark ? o : NaN, r.resolve({lowWatermark: n, watermarks: l})
                }), r.promise
            }
        }(this), r = l.defer(), a = {}, n = t.length, angular.forEach(t, function (e) {
            return function (e, t) {
                var o;
                return o = e.id, i(e).then(function (e) {
                    if (a[o] = e, t >= n - 1) return r.resolve(a)
                })
            }
        }(this)), r.promise
    }, e.hasWatermark = function (t) {
        return e.watermarks[t] && !isNaN(e.watermarks[t].lowWatermark)
    }, e.$watch("plan", function (t) {
        if (t) return c(t.nodes).then(function (t) {
            return e.watermarks = t
        })
    }), e.$on("reload", function () {
        if (e.plan) return c(e.plan.nodes).then(function (t) {
            return e.watermarks = t
        })
    })
}]).controller("JobPlanController", ["$scope", "$state", "$stateParams", "$window", "JobsService", function (e, t, r, n, o) {
    return e.nodeid = null, e.nodeUnfolded = !1, e.stateList = o.stateList(), e.changeNode = function (t) {
        return t !== e.nodeid ? (e.nodeid = t, e.vertex = null, e.subtasks = null, e.accumulators = null, e.operatorCheckpointStats = null, e.$broadcast("reload"), e.$broadcast("node:change", e.nodeid)) : (e.nodeid = null, e.nodeUnfolded = !1, e.vertex = null, e.subtasks = null, e.accumulators = null, e.operatorCheckpointStats = null)
    }, e.deactivateNode = function () {
        return e.nodeid = null, e.nodeUnfolded = !1, e.vertex = null, e.subtasks = null, e.accumulators = null, e.operatorCheckpointStats = null
    }, e.toggleFold = function () {
        return e.nodeUnfolded = !e.nodeUnfolded
    }
}]).controller("JobPlanSubtasksController", ["$scope", "JobsService", function (e, t) {
    var r;
    return e.aggregate = !1, r = function () {
        return e.aggregate ? t.getTaskManagers(e.nodeid).then(function (t) {
            return e.taskmanagers = t
        }) : t.getSubtasks(e.nodeid).then(function (t) {
            return e.subtasks = t
        })
    }, !e.nodeid || e.vertex && e.vertex.st || r(), e.$on("reload", function (t) {
        if (e.nodeid) return r()
    })
}]).controller("JobPlanAccumulatorsController", ["$scope", "JobsService", function (e, t) {
    var r;
    return r = function () {
        return t.getAccumulators(e.nodeid).then(function (t) {
            return e.accumulators = t.main, e.subtaskAccumulators = t.subtasks
        })
    }, !e.nodeid || e.vertex && e.vertex.accumulators || r(), e.$on("reload", function (t) {
        if (e.nodeid) return r()
    })
}]).controller("JobPlanCheckpointsController", ["$scope", "$state", "$stateParams", "JobsService", function (e, t, r, n) {
    var o;
    return e.checkpointDetails = {}, e.checkpointDetails.id = -1, n.getCheckpointConfig().then(function (t) {
        return e.checkpointConfig = t
    }), o = function () {
        return n.getCheckpointStats().then(function (t) {
            if (null !== t) return e.checkpointStats = t
        })
    }, o(), e.$on("reload", function (e) {
        return o()
    })
}]).controller("JobPlanCheckpointDetailsController", ["$scope", "$state", "$stateParams", "JobsService", function (e, t, r, n) {
    var o, i;
    return e.subtaskDetails = {}, e.checkpointDetails.id = r.checkpointId, o = function (t) {
        return n.getCheckpointDetails(t).then(function (t) {
            return null !== t ? e.checkpoint = t : e.unknown_checkpoint = !0
        })
    }, i = function (t, r) {
        return n.getCheckpointSubtaskDetails(t, r).then(function (t) {
            if (null !== t) return e.subtaskDetails[r] = t
        })
    }, o(r.checkpointId), e.nodeid && i(r.checkpointId, e.nodeid), e.$on("reload", function (t) {
        if (o(r.checkpointId), e.nodeid) return i(r.checkpointId, e.nodeid)
    }), e.$on("$destroy", function () {
        return e.checkpointDetails.id = -1
    })
}]).controller("JobPlanBackPressureController", ["$scope", "JobsService", function (e, t) {
    var r;
    return r = function () {
        if (e.now = Date.now(), e.nodeid) return t.getOperatorBackPressure(e.nodeid).then(function (t) {
            return e.backPressureOperatorStats[e.nodeid] = t
        })
    }, r(), e.$on("reload", function (e) {
        return r()
    })
}]).controller("JobTimelineVertexController", ["$scope", "$state", "$stateParams", "JobsService", function (e, t, r, n) {
    var o;
    return o = function () {
        return n.getVertex(r.vertexId).then(function (t) {
            return e.vertex = t
        })
    }, o(), e.$on("reload", function (e) {
        return o()
    })
}]).controller("JobExceptionsController", ["$scope", "$state", "$stateParams", "JobsService", function (e, t, r, n) {
    return n.loadExceptions().then(function (t) {
        return e.exceptions = t
    })
}]).controller("JobPropertiesController", ["$scope", "JobsService", function (e, t) {
    return e.changeNode = function (r) {
        return r !== e.nodeid ? (e.nodeid = r, t.getNode(r).then(function (t) {
            return e.node = t
        })) : (e.nodeid = null, e.node = null)
    }
}]).controller("JobPlanMetricsController", ["$scope", "JobsService", "MetricsService", function (e, t, r) {
    var n, o;
    if (e.dragging = !1, e.window = r.getWindow(), e.availableMetrics = null, e.$on("$destroy", function () {
        return r.unRegisterObserver()
    }), o = function () {
        return t.getVertex(e.nodeid).then(function (t) {
            return e.vertex = t
        }), r.getAvailableMetrics(e.jobid, e.nodeid).then(function (t) {
            return e.availableMetrics = t.sort(n), e.metrics = r.getMetricsSetup(e.jobid, e.nodeid).names, r.registerObserver(e.jobid, e.nodeid, function (t) {
                return e.$broadcast("metrics:data:update", t.timestamp, t.values)
            })
        })
    }, n = function (e, t) {
        var r, n;
        return r = e.id.toLowerCase(), n = t.id.toLowerCase(), r < n ? -1 : r > n ? 1 : 0
    }, e.dropped = function (t, n, i, a, s) {
        return r.orderMetrics(e.jobid, e.nodeid, i, n), e.$broadcast("metrics:refresh", i), o(), !1
    }, e.dragStart = function () {
        return e.dragging = !0
    }, e.dragEnd = function () {
        return e.dragging = !1
    }, e.addMetric = function (t) {
        return r.addMetric(e.jobid, e.nodeid, t.id), o()
    }, e.removeMetric = function (t) {
        return r.removeMetric(e.jobid, e.nodeid, t), o()
    }, e.setMetricSize = function (t, n) {
        return r.setMetricSize(e.jobid, e.nodeid, t, n), o()
    }, e.setMetricView = function (t, n) {
        return r.setMetricView(e.jobid, e.nodeid, t, n), o()
    }, e.getValues = function (t) {
        return r.getValues(e.jobid, e.nodeid, t)
    }, e.$on("node:change", function (t, r) {
        if (!e.dragging) return o()
    }), e.nodeid) return o()
}]), angular.module("flinkApp").directive("vertex", ["$state", function (e) {
    return {
        template: "<svg class='timeline secondary' width='0' height='0'></svg>",
        scope: {data: "="},
        link: function (e, t, r) {
            var n, o, i;
            i = t.children()[0], o = t.width(), angular.element(i).attr("width", o), (n = function (e) {
                var t, r, n;
                return d3.select(i).selectAll("*").remove(), n = [], angular.forEach(e.subtasks, function (e, t) {
                    var r;
                    return r = [{
                        label: "Scheduled",
                        color: "#666",
                        borderColor: "#555",
                        starting_time: e.timestamps.SCHEDULED,
                        ending_time: e.timestamps.DEPLOYING,
                        type: "regular"
                    }, {
                        label: "Deploying",
                        color: "#aaa",
                        borderColor: "#555",
                        starting_time: e.timestamps.DEPLOYING,
                        ending_time: e.timestamps.RUNNING,
                        type: "regular"
                    }], e.timestamps.FINISHED > 0 && r.push({
                        label: "Running",
                        color: "#ddd",
                        borderColor: "#555",
                        starting_time: e.timestamps.RUNNING,
                        ending_time: e.timestamps.FINISHED,
                        type: "regular"
                    }), n.push({label: "(" + e.subtask + ") " + e.host, times: r})
                }), t = d3.timeline().stack().tickFormat({
                    format: d3.time.format("%L"),
                    tickSize: 1
                }).prefix("single").labelFormat(function (e) {
                    return e
                }).margin({
                    left: 100,
                    right: 0,
                    top: 0,
                    bottom: 0
                }).itemHeight(30).relativeTime(), r = d3.select(i).datum(n).call(t)
            })(e.data)
        }
    }
}]).directive("timeline", ["$state", function (e) {
    return {
        template: "<svg class='timeline' width='0' height='0'></svg>",
        scope: {vertices: "=", jobid: "="},
        link: function (t, r, n) {
            var o, i, a, s;
            a = r.children()[0], i = r.width(), angular.element(a).attr("width", i), s = function (e) {
                return e.replace("&gt;", ">")
            }, o = function (r) {
                var n, o, i;
                return d3.select(a).selectAll("*").remove(), i = [], angular.forEach(r, function (e) {
                    if (e["start-time"] > -1) return "scheduled" === e.type ? i.push({
                        times: [{
                            label: s(e.name),
                            color: "#cccccc",
                            borderColor: "#555555",
                            starting_time: e["start-time"],
                            ending_time: e["end-time"],
                            type: e.type
                        }]
                    }) : i.push({
                        times: [{
                            label: s(e.name),
                            color: "#d9f1f7",
                            borderColor: "#62cdea",
                            starting_time: e["start-time"],
                            ending_time: e["end-time"],
                            link: e.id,
                            type: e.type
                        }]
                    })
                }), n = d3.timeline().stack().click(function (r, n, o) {
                    if (r.link) return e.go("single-job.timeline.vertex", {jobid: t.jobid, vertexId: r.link})
                }).tickFormat({format: d3.time.format("%L"), tickSize: 1}).prefix("main").margin({
                    left: 0,
                    right: 0,
                    top: 0,
                    bottom: 0
                }).itemHeight(30).showBorderLine().showHourTimeline(), o = d3.select(a).datum(i).call(n)
            }, t.$watch(n.vertices, function (e) {
                if (e) return o(e)
            })
        }
    }
}]).directive("split", function () {
    return {
        compile: function (e, t) {
            return Split(e.children(), {sizes: [50, 50], direction: "vertical"})
        }
    }
}).directive("jobPlan", ["$timeout", function (e) {
    return {
        template: "<svg class='graph'><g /></svg> <svg class='tmp' width='1' height='1'><g /></svg> <div class='btn-group zoom-buttons'> <a class='btn btn-default zoom-in' ng-click='zoomIn()'><i class='fa fa-plus' /></a> <a class='btn btn-default zoom-out' ng-click='zoomOut()'><i class='fa fa-minus' /></a> </div>",
        scope: {plan: "=", watermarks: "=", setNode: "&"},
        link: function (e, t, r) {
            var n, o, i, a, s, l, u, c, d, f, p, m, g, h, v, b, k, j, S, w, C, $, y, M, J;
            p = null, C = d3.behavior.zoom(), J = [], h = r.jobid, S = t.children()[0], j = t.children().children()[0], w = t.children()[1], l = d3.select(S), u = d3.select(j), c = d3.select(w), n = t.width(), angular.element(t.children()[0]).width(n), b = 0, v = 0, e.zoomIn = function () {
                var e, t, r;
                if (C.scale() < 2.99) return e = C.translate(), t = e[0] * (C.scale() + .1 / C.scale()), r = e[1] * (C.scale() + .1 / C.scale()), C.scale(C.scale() + .1), C.translate([t, r]), u.attr("transform", "translate(" + t + "," + r + ") scale(" + C.scale() + ")"), b = C.scale(), v = C.translate()
            }, e.zoomOut = function () {
                var e, t, r;
                if (C.scale() > .31) return C.scale(C.scale() - .1), e = C.translate(), t = e[0] * (C.scale() - .1 / C.scale()), r = e[1] * (C.scale() - .1 / C.scale()), C.translate([t, r]), u.attr("transform", "translate(" + t + "," + r + ") scale(" + C.scale() + ")"), b = C.scale(), v = C.translate()
            }, i = function (e) {
                var t;
                return t = "", null == e.ship_strategy && null == e.local_strategy || (t += "<div class='edge-label'>", null != e.ship_strategy && (t += e.ship_strategy), void 0 !== e.temp_mode && (t += " (" + e.temp_mode + ")"), void 0 !== e.local_strategy && (t += ",<br>" + e.local_strategy), t += "</div>"), t
            }, g = function (e) {
                return "partialSolution" === e || "nextPartialSolution" === e || "workset" === e || "nextWorkset" === e || "solutionSet" === e || "solutionDelta" === e
            }, m = function (e, t) {
                return "mirror" === t ? "node-mirror" : g(t) ? "node-iteration" : "node-normal"
            }, a = function (e, t, r, n) {
                var o, i;
                return o = "<div href='#/jobs/" + h + "/vertex/" + e.id + "' class='node-label " + m(e, t) + "'>", o += "mirror" === t ? "<h3 class='node-name'>Mirror of " + e.operator + "</h3>" : "<h3 class='node-name'>" + e.operator + "</h3>", "" === e.description ? o += "" : (i = e.description, i = M(i), o += "<h4 class='step-name'>" + i + "</h4>"), null != e.step_function ? o += f(e.id, r, n) : (g(t) && (o += "<h5>" + t + " Node</h5>"), "" !== e.parallelism && (o += "<h5>Parallelism: " + e.parallelism + "</h5>"), void 0 !== e.lowWatermark && (o += "<h5>Low Watermark: " + e.lowWatermark + "</h5>"), void 0 !== e.operator && e.operator_strategy && (o += "<h5>Operation: " + M(e.operator_strategy) + "</h5>")), o += "</div>"
            }, f = function (e, t, r) {
                var n, o;
                return o = "svg-" + e, n = "<svg class='" + o + "' width=" + t + " height=" + r + "><g /></svg>"
            }, M = function (e) {
                var t;
                for ("<" === e.charAt(0) && (e = e.replace("<", "&lt;"), e = e.replace(">", "&gt;")), t = ""; e.length > 30;) t = t + e.substring(0, 30) + "<br>", e = e.substring(30, e.length);
                return t += e
            }, s = function (e, t, r, n, o, i) {
                return null == n && (n = !1), r.id === t.partial_solution ? e.setNode(r.id, {
                    label: a(r, "partialSolution", o, i),
                    labelType: "html",
                    "class": m(r, "partialSolution")
                }) : r.id === t.next_partial_solution ? e.setNode(r.id, {
                    label: a(r, "nextPartialSolution", o, i),
                    labelType: "html",
                    "class": m(r, "nextPartialSolution")
                }) : r.id === t.workset ? e.setNode(r.id, {
                    label: a(r, "workset", o, i),
                    labelType: "html",
                    "class": m(r, "workset")
                }) : r.id === t.next_workset ? e.setNode(r.id, {
                    label: a(r, "nextWorkset", o, i),
                    labelType: "html",
                    "class": m(r, "nextWorkset")
                }) : r.id === t.solution_set ? e.setNode(r.id, {
                    label: a(r, "solutionSet", o, i),
                    labelType: "html",
                    "class": m(r, "solutionSet")
                }) : r.id === t.solution_delta ? e.setNode(r.id, {
                    label: a(r, "solutionDelta", o, i),
                    labelType: "html",
                    "class": m(r, "solutionDelta")
                }) : e.setNode(r.id, {label: a(r, "", o, i), labelType: "html", "class": m(r, "")})
            }, o = function (e, t, r, n, o) {
                return e.setEdge(o.id, r.id, {label: i(o), labelType: "html", arrowhead: "normal"})
            }, k = function (e, t) {
                var r, n, i, a, l, u, d, f, p, m, g, h, v, b;
                for (n = [], null != t.nodes ? b = t.nodes : (b = t.step_function, i = !0), a = 0, u = b.length; a < u; a++) if (r = b[a], p = 0, f = 0, r.step_function && (v = new dagreD3.graphlib.Graph({
                    multigraph: !0,
                    compound: !0
                }).setGraph({
                    nodesep: 20,
                    edgesep: 0,
                    ranksep: 20,
                    rankdir: "LR",
                    marginx: 10,
                    marginy: 10
                }), J[r.id] = v, k(v, r), g = new dagreD3.render, c.select("g").call(g, v), p = v.graph().width, f = v.graph().height, angular.element(w).empty()), s(e, t, r, i, p, f), n.push(r.id), null != r.inputs) for (h = r.inputs, l = 0, d = h.length; l < d; l++) m = h[l], o(e, t, r, n, m);
                return e
            }, y = function (e, t) {
                var r, n, o;
                for (n in e.nodes) {
                    if (r = e.nodes[n], r.id === t) return r;
                    if (null != r.step_function) for (o in r.step_function) if (r.step_function[o].id === t) return r.step_function[o]
                }
            }, $ = function (e, t) {
                var r, n, o, i;
                if (!_.isEmpty(t)) for (i = e.nodes, r = 0, n = i.length; r < n; r++) o = i[r], t[o.id] && !isNaN(t[o.id].lowWatermark) && (o.lowWatermark = t[o.id].lowWatermark);
                return e
            }, v = 0, b = 0, d = function () {
                var t, r, n, o, i, a;
                if (e.plan) {
                    p = new dagreD3.graphlib.Graph({multigraph: !0, compound: !0}).setGraph({
                        nodesep: 70,
                        edgesep: 0,
                        ranksep: 50,
                        rankdir: "LR",
                        marginx: 40,
                        marginy: 40
                    }), k(p, $(e.plan, e.watermarks)), u.selectAll("*").remove(), u.attr("transform", "scale(1)"), n = new dagreD3.render, u.call(n, p);
                    for (t in J) o = J[t], l.select("svg.svg-" + t + " g").call(n, o);
                    return r = .5, i = Math.floor((angular.element(S).width() - p.graph().width * r) / 2), a = Math.floor((angular.element(S).height() - p.graph().height * r) / 2), 0 !== b && 0 !== v ? (C.scale(b).translate(v), u.attr("transform", "translate(" + v + ") scale(" + b + ")")) : (C.scale(r).translate([i, a]), u.attr("transform", "translate(" + i + ", " + a + ") scale(" + C.scale() + ")")), C.on("zoom", function () {
                        var e;
                        return e = d3.event, b = e.scale, v = e.translate, u.attr("transform", "translate(" + v + ") scale(" + b + ")")
                    }), C(l), u.selectAll(".node").on("click", function (t) {
                        return e.setNode({nodeid: t})
                    })
                }
            }, e.$watch(r.plan, function (e) {
                if (e) return d()
            }), e.$watch(r.watermarks, function (t) {
                if (t && e.plan) return d()
            })
        }
    }
}]), angular.module("flinkApp").service("JobsService", ["$http", "flinkConfig", "$log", "amMoment", "$q", "$timeout", function (e, t, r, n, o, i) {
    var a, s, l, u, c, d;
    return a = null, s = null, l = {}, c = {
        running: [],
        finished: [],
        cancelled: [],
        failed: []
    }, u = [], d = function () {
        return angular.forEach(u, function (e) {
            return e()
        })
    }, this.registerObserver = function (e) {
        return u.push(e)
    }, this.unRegisterObserver = function (e) {
        var t;
        return t = u.indexOf(e), u.splice(t, 1)
    }, this.stateList = function () {
        return ["SCHEDULED", "DEPLOYING", "RUNNING", "FINISHED", "FAILED", "CANCELING", "CANCELED"]
    }, this.translateLabelState = function (e) {
        switch (e.toLowerCase()) {
            case"finished":
                return "success";
            case"failed":
                return "danger";
            case"scheduled":
                return "default";
            case"deploying":
                return "info";
            case"running":
                return "primary";
            case"canceling":
                return "warning";
            case"pending":
                return "info";
            case"total":
                return "black";
            default:
                return "default"
        }
    }, this.setEndTimes = function (e) {
        return angular.forEach(e, function (e, t) {
            if (!(e["end-time"] > -1)) return e["end-time"] = e["start-time"] + e.duration
        })
    }, this.processVertices = function (e) {
        return angular.forEach(e.vertices, function (e, t) {
            return e.type = "regular"
        }), e.vertices.unshift({
            name: "Scheduled",
            "start-time": e.timestamps.CREATED,
            "end-time": e.timestamps.CREATED + 1,
            type: "scheduled"
        })
    }, this.listJobs = function () {
        var r;
        return r = o.defer(), e.get(t.jobServer + "jobs/overview").success(function (e) {
            return function (t, n, o, i) {
                return c.finished = [], c.running = [], _(t.jobs).groupBy(function (e) {
                    switch (e.state.toLowerCase()) {
                        case"finished":
                            return "finished";
                        case"failed":
                            return "finished";
                        case"canceled":
                            return "finished";
                        default:
                            return "running"
                    }
                }).forEach(function (t, r) {
                    switch (r) {
                        case"finished":
                            return c.finished = e.setEndTimes(t);
                        case"running":
                            return c.running = e.setEndTimes(t)
                    }
                }).value(), r.resolve(c), d()
            }
        }(this)), r.promise
    }, this.getJobs = function (e) {
        return c[e]
    }, this.getAllJobs = function () {
        return c
    }, this.loadJob = function (r) {
        return a = null, l.job = o.defer(), e.get(t.jobServer + "jobs/" + r).success(function (n) {
            return function (o, i, s, u) {
                return n.setEndTimes(o.vertices), n.processVertices(o), e.get(t.jobServer + "jobs/" + r + "/config").success(function (e) {
                    return o = angular.extend(o, e), a = o, l.job.resolve(a)
                })
            }
        }(this)), l.job.promise
    }, this.getNode = function (e) {
        var t, r;
        return r = function (e, t) {
            var n, o, i, a;
            for (n = 0, o = t.length; n < o; n++) {
                if (i = t[n], i.id === e) return i;
                if (i.step_function && (a = r(e, i.step_function)), a) return a
            }
            return null
        }, t = o.defer(), l.job.promise.then(function (n) {
            return function (o) {
                var i;
                return i = r(e, a.plan.nodes), i.vertex = n.seekVertex(e), t.resolve(i)
            }
        }(this)), t.promise
    }, this.seekVertex = function (e) {
        var t, r, n, o;
        for (n = a.vertices, t = 0, r = n.length; t < r; t++) if (o = n[t], o.id === e) return o;
        return null
    }, this.getVertex = function (r) {
        var n;
        return n = o.defer(), l.job.promise.then(function (o) {
            return function (i) {
                var s;
                return s = o.seekVertex(r), e.get(t.jobServer + "jobs/" + a.jid + "/vertices/" + r + "/subtasktimes").success(function (e) {
                    return s.subtasks = e.subtasks, n.resolve(s)
                })
            }
        }(this)), n.promise
    }, this.getSubtasks = function (r) {
        var n;
        return n = o.defer(), l.job.promise.then(function (o) {
            return function (o) {
                return e.get(t.jobServer + "jobs/" + a.jid + "/vertices/" + r).success(function (e) {
                    var t;
                    return t = e.subtasks, n.resolve(t)
                })
            }
        }(this)), n.promise
    }, this.getTaskManagers = function (r) {
        var n;
        return n = o.defer(), l.job.promise.then(function (o) {
            return function (o) {
                return e.get(t.jobServer + "jobs/" + a.jid + "/vertices/" + r + "/taskmanagers").success(function (e) {
                    var t;
                    return t = e.taskmanagers, n.resolve(t)
                })
            }
        }(this)), n.promise
    }, this.getAccumulators = function (r) {
        var n;
        return n = o.defer(), l.job.promise.then(function (o) {
            return function (o) {
                return console.log(a.jid), e.get(t.jobServer + "jobs/" + a.jid + "/vertices/" + r + "/accumulators").success(function (o) {
                    var i;
                    return i = o["user-accumulators"], e.get(t.jobServer + "jobs/" + a.jid + "/vertices/" + r + "/subtasks/accumulators").success(function (e) {
                        var t;
                        return t = e.subtasks, n.resolve({main: i, subtasks: t})
                    })
                })
            }
        }(this)), n.promise
    }, this.getCheckpointConfig = function () {
        var r;
        return r = o.defer(), l.job.promise.then(function (n) {
            return function (n) {
                return e.get(t.jobServer + "jobs/" + a.jid + "/checkpoints/config").success(function (e) {
                    return angular.equals({}, e) ? r.resolve(null) : r.resolve(e)
                })
            }
        }(this)), r.promise
    }, this.getCheckpointStats = function () {
        var r;
        return r = o.defer(), l.job.promise.then(function (n) {
            return function (n) {
                return e.get(t.jobServer + "jobs/" + a.jid + "/checkpoints").success(function (e, t, n, o) {
                    return angular.equals({}, e) ? r.resolve(null) : r.resolve(e)
                })
            }
        }(this)), r.promise
    }, this.getCheckpointDetails = function (r) {
        var n;
        return n = o.defer(), l.job.promise.then(function (o) {
            return function (o) {
                return e.get(t.jobServer + "jobs/" + a.jid + "/checkpoints/details/" + r).success(function (e) {
                    return angular.equals({}, e) ? n.resolve(null) : n.resolve(e)
                })
            }
        }(this)), n.promise
    }, this.getCheckpointSubtaskDetails = function (r, n) {
        var i;
        return i = o.defer(), l.job.promise.then(function (o) {
            return function (o) {
                return e.get(t.jobServer + "jobs/" + a.jid + "/checkpoints/details/" + r + "/subtasks/" + n).success(function (e) {
                    return angular.equals({}, e) ? i.resolve(null) : i.resolve(e)
                })
            }
        }(this)), i.promise
    }, this.getOperatorBackPressure = function (r) {
        var n;
        return n = o.defer(), e.get(t.jobServer + "jobs/" + a.jid + "/vertices/" + r + "/backpressure").success(function (e) {
            return function (e) {
                return n.resolve(e)
            }
        }(this)), n.promise
    }, this.translateBackPressureLabelState = function (e) {
        switch (e.toLowerCase()) {
            case"in-progress":
                return "danger";
            case"ok":
                return "success";
            case"low":
                return "warning";
            case"high":
                return "danger";
            default:
                return "default"
        }
    }, this.loadExceptions = function () {
        var r;
        return r = o.defer(), l.job.promise.then(function (n) {
            return function (n) {
                return e.get(t.jobServer + "jobs/" + a.jid + "/exceptions").success(function (e) {
                    return a.exceptions = e, r.resolve(e)
                })
            }
        }(this)), r.promise
    }, this.cancelJob = function (r) {
        return e.get(t.jobServer + "jobs/" + r + "/yarn-cancel")
    }, this
}]), angular.module("flinkApp").directive("metricsGraph", function () {
    return {
        template: '<div class="panel panel-default panel-metric"> <div class="panel-heading"> <span class="metric-title">{{metric.id}}</span> <div class="buttons"> <div class="btn-group"> <button type="button" ng-class="[btnClasses, {active: metric.size != \'big\'}]" ng-click="setSize(\'small\')">Small</button> <button type="button" ng-class="[btnClasses, {active: metric.size == \'big\'}]" ng-click="setSize(\'big\')">Big</button> </div> <a title="Remove" class="btn btn-default btn-xs remove" ng-click="removeMetric()"><i class="fa fa-close" /></a> </div> </div> <div class="panel-body"> <svg ng-if="metric.view == \'chart\'"/> <div ng-if="metric.view != \'chart\'"> <div class="metric-numeric" title="{{value | humanizeChartNumericTitle:metric}}">{{value | humanizeChartNumeric:metric}}</div> </div> </div> <div class="buttons"> <div class="btn-group"> <button type="button" ng-class="[btnClasses, {active: metric.view == \'chart\'}]" ng-click="setView(\'chart\')">Chart</button> <button type="button" ng-class="[btnClasses, {active: metric.view != \'chart\'}]" ng-click="setView(\'numeric\')">Numeric</button> </div> </div>',
        replace: !0,
        scope: {metric: "=", window: "=", removeMetric: "&", setMetricSize: "=", setMetricView: "=", getValues: "&"},
        link: function (e, t, r) {
            return e.btnClasses = ["btn", "btn-default", "btn-xs"], e.value = null, e.data = [{values: e.getValues()}], e.options = {
                x: function (e, t) {
                    return e.x
                }, y: function (e, t) {
                    return e.y
                }, xTickFormat: function (e) {
                    return d3.time.format("%H:%M:%S")(new Date(e))
                }, yTickFormat: function (e) {
                    var t, r, n, o;
                    for (r = !1, n = 0, o = 1, t = Math.abs(e); !r && n < 50;) Math.pow(10, n) <= t && t < Math.pow(10, n + o) ? r = !0 : n += o;
                    return r && n > 6 ? e / Math.pow(10, n) + "E" + n : "" + e
                }
            }, e.showChart = function () {
                return d3.select(t.find("svg")[0]).datum(e.data).transition().duration(250).call(e.chart)
            }, e.chart = nv.models.lineChart().options(e.options).showLegend(!1).margin({
                top: 15,
                left: 60,
                bottom: 30,
                right: 30
            }), e.chart.yAxis.showMaxMin(!1), e.chart.tooltip.hideDelay(0), e.chart.tooltip.contentGenerator(function (e) {
                return "<p>" + d3.time.format("%H:%M:%S")(new Date(e.point.x)) + " | " + e.point.y + "</p>"
            }), nv.utils.windowResize(e.chart.update), e.setSize = function (t) {
                return e.setMetricSize(e.metric, t)
            }, e.setView = function (t) {
                if (e.setMetricView(e.metric, t), "chart" === t) return e.showChart()
            }, "chart" === e.metric.view && e.showChart(), e.$on("metrics:data:update", function (t, r, n) {
                return e.value = parseFloat(n[e.metric.id]), e.data[0].values.push({
                    x: r,
                    y: e.value
                }), e.data[0].values.length > e.window && e.data[0].values.shift(), "chart" === e.metric.view && e.showChart(), "chart" === e.metric.view && e.chart.clearHighlights(), e.chart.tooltip.hidden(!0)
            }), t.find(".metric-title").qtip({
                content: {text: e.metric.id},
                position: {my: "bottom left", at: "top left"},
                style: {classes: "qtip-light qtip-timeline-bar"}
            })
        }
    }
}), angular.module("flinkApp").service("MetricsService", ["$http", "$q", "flinkConfig", "$interval", function (e, t, r, n) {
    return this.metrics = {}, this.values = {}, this.watched = {}, this.observer = {
        jobid: null,
        nodeid: null,
        callback: null
    }, this.refresh = n(function (e) {
        return function () {
            return angular.forEach(e.metrics, function (t, r) {
                return angular.forEach(t, function (t, n) {
                    var o;
                    if (o = [], angular.forEach(t, function (e, t) {
                        return o.push(e.id)
                    }), o.length > 0) return e.getMetrics(r, n, o).then(function (t) {
                        if (r === e.observer.jobid && n === e.observer.nodeid && e.observer.callback) return e.observer.callback(t)
                    })
                })
            })
        }
    }(this), r["refresh-interval"]), this.registerObserver = function (e, t, r) {
        return this.observer.jobid = e, this.observer.nodeid = t, this.observer.callback = r
    }, this.unRegisterObserver = function () {
        return this.observer = {jobid: null, nodeid: null, callback: null}
    }, this.setupMetrics = function (e, t) {
        return this.setupLS(), this.watched[e] = [], angular.forEach(t, function (t) {
            return function (r, n) {
                if (r.id) return t.watched[e].push(r.id)
            }
        }(this))
    }, this.getWindow = function () {
        return 100
    }, this.setupLS = function () {
        return null == sessionStorage.flinkMetrics && this.saveSetup(), this.metrics = JSON.parse(sessionStorage.flinkMetrics)
    }, this.saveSetup = function () {
        return sessionStorage.flinkMetrics = JSON.stringify(this.metrics)
    }, this.saveValue = function (e, t, r) {
        if (null == this.values[e] && (this.values[e] = {}), null == this.values[e][t] && (this.values[e][t] = []), this.values[e][t].push(r), this.values[e][t].length > this.getWindow()) return this.values[e][t].shift()
    }, this.getValues = function (e, t, r) {
        var n;
        return null == this.values[e] ? [] : null == this.values[e][t] ? [] : (n = [], angular.forEach(this.values[e][t], function (e) {
            return function (e, t) {
                if (null != e.values[r]) return n.push({x: e.timestamp, y: e.values[r]})
            }
        }(this)), n)
    }, this.setupLSFor = function (e, t) {
        if (null == this.metrics[e] && (this.metrics[e] = {}), null == this.metrics[e][t]) return this.metrics[e][t] = []
    }, this.addMetric = function (e, t, r) {
        return this.setupLSFor(e, t), this.metrics[e][t].push({id: r, size: "small", view: "chart"}), this.saveSetup()
    }, this.removeMetric = function (e) {
        return function (t, r, n) {
            var o;
            if (null != e.metrics[t][r]) return o = e.metrics[t][r].indexOf(n), o === -1 && (o = _.findIndex(e.metrics[t][r], {id: n})), o !== -1 && e.metrics[t][r].splice(o, 1), e.saveSetup()
        }
    }(this), this.setMetricSize = function (e) {
        return function (t, r, n, o) {
            var i;
            if (null != e.metrics[t][r]) return i = e.metrics[t][r].indexOf(n.id), i === -1 && (i = _.findIndex(e.metrics[t][r], {id: n.id})), i !== -1 && (e.metrics[t][r][i] = {
                id: n.id,
                size: o,
                view: n.view
            }), e.saveSetup()
        }
    }(this), this.setMetricView = function (e) {
        return function (t, r, n, o) {
            var i;
            if (null != e.metrics[t][r]) return i = e.metrics[t][r].indexOf(n.id), i === -1 && (i = _.findIndex(e.metrics[t][r], {id: n.id})), i !== -1 && (e.metrics[t][r][i] = {
                id: n.id,
                size: n.size,
                view: o
            }), e.saveSetup()
        }
    }(this), this.orderMetrics = function (e, t, r, n) {
        return this.setupLSFor(e, t), angular.forEach(this.metrics[e][t], function (o) {
            return function (i, a) {
                if (i.id === r.id && (o.metrics[e][t].splice(a, 1), a < n)) return n -= 1
            }
        }(this)), this.metrics[e][t].splice(n, 0, r), this.saveSetup()
    }, this.getMetricsSetup = function (e) {
        return function (t, r) {
            return {
                names: _.map(e.metrics[t][r], function (e) {
                    return _.isString(e) ? {id: e, size: "small", view: "chart"} : e
                })
            }
        }
    }(this), this.getAvailableMetrics = function (n) {
        return function (o, i) {
            var a;
            return n.setupLSFor(o, i), a = t.defer(), e.get(r.jobServer + "jobs/" + o + "/vertices/" + i + "/metrics").success(function (e) {
                var t;
                return t = [], angular.forEach(e, function (e, r) {
                    var a;
                    if (a = n.metrics[o][i].indexOf(e.id), a === -1 && (a = _.findIndex(n.metrics[o][i], {id: e.id})), a === -1) return t.push(e)
                }), a.resolve(t)
            }), a.promise
        }
    }(this), this.getAllAvailableMetrics = function (n) {
        return function (n, o) {
            var i;
            return i = t.defer(), e.get(r.jobServer + "jobs/" + n + "/vertices/" + o + "/metrics").success(function (e) {
                return i.resolve(e)
            }), i.promise
        }
    }(this), this.getMetrics = function (n, o, i) {
        var a, s;
        return a = t.defer(), s = i.join(","), e.get(r.jobServer + "jobs/" + n + "/vertices/" + o + "/metrics?get=" + s).success(function (e) {
            return function (t) {
                var r, i;
                return i = {}, angular.forEach(t, function (e, t) {
                    return i[e.id] = parseInt(e.value)
                }), r = {timestamp: Date.now(), values: i}, e.saveValue(n, o, r), a.resolve(r)
            }
        }(this)), a.promise
    }, this.setupLS(), this
}]), angular.module("flinkApp").controller("OverviewController", ["$scope", "OverviewService", "JobsService", "$interval", "flinkConfig", function (e, t, r, n, o) {
    var i;
    return e.jobObserver = function () {
        return e.runningJobs = r.getJobs("running"), e.finishedJobs = r.getJobs("finished")
    }, r.registerObserver(e.jobObserver), e.$on("$destroy", function () {
        return r.unRegisterObserver(e.jobObserver)
    }), e.jobObserver(), t.loadOverview().then(function (t) {
        return e.overview = t
    }), i = n(function () {
        return t.loadOverview().then(function (t) {
            return e.overview = t
        })
    }, o["refresh-interval"]), e.$on("$destroy", function () {
        return n.cancel(i)
    })
}]), angular.module("flinkApp").service("OverviewService", ["$http", "flinkConfig", "$q", function (e, t, r) {
    var n;
    return n = {}, this.loadOverview = function () {
        var o;
        return o = r.defer(), e.get(t.jobServer + "overview").success(function (e, t, r, i) {
            return n = e, o.resolve(e)
        }), o.promise
    }, this
}]), angular.module("flinkApp").controller("JobSubmitController", ["$scope", "JobSubmitService", "$interval", "flinkConfig", "$state", "$location", function (e, t, r, n, o, i) {
    var a;
    return e.yarn = i.absUrl().indexOf("/proxy/application_") !== -1, e.loadList = function () {
        return t.loadJarList().then(function (t) {
            return e.address = t.address, null != t.error ? e.noaccess = t.error : null != t.errors && (e.noaccess = t.errors[0]), e.jars = t.files
        })
    }, e.defaultState = function () {
        return e.plan = null, e.error = null, e.state = {
            selected: null,
            parallelism: "",
            savepointPath: "",
            allowNonRestoredState: !1,
            "entry-class": "",
            "program-args": "",
            "plan-button": "Show Plan",
            "submit-button": "Submit",
            "action-time": 0
        }
    }, e.defaultState(), e.uploader = {}, e.loadList(), a = r(function () {
        return e.loadList()
    }, n["refresh-interval"]), e.$on("$destroy", function () {
        return r.cancel(a)
    }), e.selectJar = function (t) {
        return e.state.selected === t ? e.defaultState() : (e.defaultState(), e.state.selected = t)
    }, e.deleteJar = function (r, n) {
        return e.state.selected === n && e.defaultState(), angular.element(r.currentTarget).removeClass("fa-remove").addClass("fa-spin fa-spinner"), t.deleteJar(n).then(function (e) {
            return angular.element(r.currentTarget).removeClass("fa-spin fa-spinner").addClass("fa-remove"), null != e.error ? alert(e.error) : null != e.errors ? alert(e.errors[0]) : void 0
        })
    }, e.loadEntryClass = function (t) {
        return e.state["entry-class"] = t
    }, e.getPlan = function () {
        var r, n;
        if ("Show Plan" === e.state["plan-button"]) return r = (new Date).getTime(), e.state["action-time"] = r, e.state["submit-button"] = "Submit", e.state["plan-button"] = "Getting Plan", e.error = null, e.plan = null, n = {}, e.state["entry-class"] && (n["entry-class"] = e.state["entry-class"]), e.state.parallelism && (n.parallelism = e.state.parallelism), e.state["program-args"] && (n["program-args"] = e.state["program-args"]), t.getPlan(e.state.selected, n).then(function (t) {
            if (r === e.state["action-time"]) return e.state["plan-button"] = "Show Plan", null != t.error ? e.error = t.error : null != t.errors && (e.error = t.errors[0]), e.plan = t.plan
        })["catch"](function (t) {
            return e.state["plan-button"] = "Show Plan", e.error = t
        })
    }, e.runJob = function () {
        var r, n, i;
        if ("Submit" === e.state["submit-button"]) return r = (new Date).getTime(), e.state["action-time"] = r, e.state["submit-button"] = "Submitting", e.state["plan-button"] = "Show Plan", e.error = null, i = {}, n = {}, e.state["entry-class"] && (i.entryClass = e.state["entry-class"], n["entry-class"] = e.state["entry-class"]), e.state.parallelism && (i.parallelism = e.state.parallelism, n.parallelism = e.state.parallelism), e.state["program-args"] && (i.programArgs = e.state["program-args"], n["program-args"] = e.state["program-args"]), e.state.savepointPath && (i.savepointPath = e.state.savepointPath, n.savepointPath = e.state.savepointPath), e.state.allowNonRestoredState && (i.allowNonRestoredState = e.state.allowNonRestoredState, n.allowNonRestoredState = e.state.allowNonRestoredState), t.runJob(e.state.selected, i, n).then(function (t) {
            if (r === e.state["action-time"] && (e.state["submit-button"] = "Submit", null != t.error ? e.error = t.error : null != t.errors && (e.error = t.errors[0]), null != t.jobid)) return o.go("single-job.plan.subtasks", {jobid: t.jobid})
        })["catch"](function (t) {
            return e.state["submit-button"] = "Submit", e.error = t
        })
    }, e.nodeid = null, e.changeNode = function (t) {
        return t !== e.nodeid ? (e.nodeid = t, e.vertex = null, e.subtasks = null, e.accumulators = null, e.$broadcast("reload")) : (e.nodeid = null, e.nodeUnfolded = !1, e.vertex = null, e.subtasks = null, e.accumulators = null)
    }, e.clearFiles = function () {
        return e.uploader = {}
    }, e.uploadFiles = function (t) {
        return e.uploader = {}, 1 === t.length ? (e.uploader.file = t[0], e.uploader.upload = !0) : e.uploader.error = "Did ya forget to select a file?"
    }, e.startUpload = function () {
        var t, r;
        return null != e.uploader.file ? (t = new FormData, t.append("jarfile", e.uploader.file), e.uploader.upload = !1, e.uploader.success = "Initializing upload...", r = new XMLHttpRequest, r.upload.onprogress = function (t) {
            return e.uploader.success = null, e.uploader.progress = parseInt(100 * t.loaded / t.total)
        }, r.upload.onerror = function (t) {
            return e.uploader.progress = null, e.uploader.error = "An error occurred while uploading your file"
        }, r.upload.onload = function (t) {
            return e.uploader.progress = null, e.uploader.success = "Saving..."
        }, r.onreadystatechange = function () {
            var t;
            if (4 === r.readyState) return t = JSON.parse(r.responseText), null != t.error ? (e.uploader.error = t.error, e.uploader.success = null) : null != t.errors ? (e.uploader.error = t.errors[0], e.uploader.success = null) : e.uploader.success = "Uploaded!"
        }, r.open("POST", n.jobServer + "jars/upload"), r.send(t)) : console.log("Unexpected Error. This should not happen")
    }
}]).filter("getJarSelectClass", function () {
    return function (e, t) {
        return e === t ? "fa-check-square" : "fa-square-o"
    }
}), angular.module("flinkApp").service("JobSubmitService", ["$http", "flinkConfig", "$q", function (e, t, r) {
    return this.loadJarList = function () {
        var n;
        return n = r.defer(), e.get(t.jobServer + "jars/").success(function (e, t, r, o) {
            return n.resolve(e)
        }), n.promise
    }, this.deleteJar = function (n) {
        var o;
        return o = r.defer(), e["delete"](t.jobServer + "jars/" + encodeURIComponent(n)).success(function (e, t, r, n) {
            return o.resolve(e)
        }), o.promise
    }, this.getPlan = function (n, o) {
        var i;
        return i = r.defer(), e.get(t.jobServer + "jars/" + encodeURIComponent(n) + "/plan", {params: o}).success(function (e, t, r, n) {
            return i.resolve(e)
        }).error(function (e) {
            return null != e.errors ? i.reject(e.errors[0]) : i.reject(e)
        }), i.promise
    }, this.runJob = function (n, o, i) {
        var a;
        return a = r.defer(), e.post(t.jobServer + "jars/" + encodeURIComponent(n) + "/run", o, {params: i}).success(function (e, t, r, n) {
            return a.resolve(e)
        }).error(function (e) {
            return null != e.errors ? a.reject(e.errors[0]) : a.reject(e)
        }), a.promise
    }, this
}]), angular.module("flinkApp").controller("AllTaskManagersController", ["$scope", "TaskManagersService", "$interval", "flinkConfig", function (e, t, r, n) {
    var o;
    return t.loadManagers().then(function (t) {
        return e.managers = t
    }), o = r(function () {
        return t.loadManagers().then(function (t) {
            return e.managers = t
        })
    }, n["refresh-interval"]), e.$on("$destroy", function () {
        return r.cancel(o)
    })
}]).controller("SingleTaskManagerController", ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function (e, t, r, n, o) {
    var i;
    return e.metrics = {}, r.loadMetrics(t.taskmanagerid).then(function (t) {
        return e.metrics = t
    }), i = n(function () {
        return r.loadMetrics(t.taskmanagerid).then(function (t) {
            return e.metrics = t
        })
    }, o["refresh-interval"]), e.$on("$destroy", function () {
        return n.cancel(i)
    })
}]).controller("SingleTaskManagerLogsController", ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function (e, t, r, n, o) {
    return e.log = {}, e.taskmanagerid = t.taskmanagerid, r.loadLogs(t.taskmanagerid).then(function (t) {
        return e.log = t
    }), e.reloadData = function () {
        return r.loadLogs(t.taskmanagerid).then(function (t) {
            return e.log = t
        })
    }
}]).controller("SingleTaskManagerDumpController", ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function (e, t, r, n, o) {
    return e.dump = {}, e.taskmanagerid = t.taskmanagerid, r.loadDump(t.taskmanagerid).then(function (t) {
        return e.dump = t
    }), e.reloadData = function () {
        return r.loadDump(t.taskmanagerid).then(function (t) {
            return e.dump = t
        })
    }
}]).controller("SingleTaskManagerStdoutController", ["$scope", "$stateParams", "SingleTaskManagerService", "$interval", "flinkConfig", function (e, t, r, n, o) {
    return e.stdout = {}, e.taskmanagerid = t.taskmanagerid, r.loadStdout(t.taskmanagerid).then(function (t) {
        return e.stdout = t
    }), e.reloadData = function () {
        return r.loadStdout(t.taskmanagerid).then(function (t) {
            return e.stdout = t
        })
    }
}]), angular.module("flinkApp").service("TaskManagersService", ["$http", "flinkConfig", "$q", function (e, t, r) {
    return this.loadManagers = function () {
        var n;
        return n = r.defer(), e.get(t.jobServer + "taskmanagers").success(function (e, t, r, o) {
            return n.resolve(e.taskmanagers)
        }), n.promise
    }, this
}]).service("SingleTaskManagerService", ["$http", "flinkConfig", "$q", function (e, t, r) {
    return this.loadMetrics = function (n) {
        var o;
        return o = r.defer(), e.get(t.jobServer + "taskmanagers/" + n).success(function (e, t, r, n) {
            return o.resolve(e)
        }), o.promise
    }, this.loadLogs = function (n) {
        var o;
        return o = r.defer(), e.get(t.jobServer + "taskmanagers/" + n + "/log").success(function (e, t, r, n) {
            return o.resolve(e)
        }), o.promise
    }, this.loadDump = function (n) {
        var o;
        return o = r.defer(), e.get(t.jobServer + "taskmanagers/" + n + "/dump").success(function (e, t, r, n) {
            return o.resolve(e)
        }), o.promise
    }, this.loadStdout = function (n) {
        var o;
        return o = r.defer(), e.get(t.jobServer + "taskmanagers/" + n + "/stdout").success(function (e, t, r, n) {
            return o.resolve(e)
        }), o.promise
    }, this
}]);
