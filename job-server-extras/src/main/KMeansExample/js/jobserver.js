var jobserverURL = window.location.href.replace(/:5080.*/, ":8090");
var cluster_labels;
var data;
var pageGraphicsDrawnYet = false;

function reloadPageGraphics(labelsAndData) {
	cluster_labels = labelsAndData[0];
	data = labelsAndData[1].map(JSON.parse);
	if (!pageGraphicsDrawnYet) {
		//build option list that will be converted to dropdown
       	for (var i = 0; i < cluster_labels.length; i++) {
        	$('<option>', {
        		value: cluster_labels[i]
        		}).text(cluster_labels[i]).appendTo("#multiSelect");
        }
        //enable multiSelect dropdown
        $('#multiSelect').multiselect({
        	enableFiltering: true,
        	includeSelectAllOption: true,
        	selectedClass: 'multiselect-selected',
        });
        //on click, update url
        $('input[type="checkbox"]').click(function() {
        	window.history.pushState({}, "",URI(window.location.href).setQuery({active: getActiveLabels()}).toString())
        });
        //select those which are in URL
       	$('#multiSelect').multiselect('select', URI(window.location.href).query(true)['active']);
	}
	pageGraphicsDrawnYet = true;
	drawData();
}

function getActiveLabels() {
	return $.makeArray($('label > :checked').map(function() {
		return this.value
	})).filter(function(val) {
		return val !== 'multiselect-all'
	});
}

function startContext() 
{	$("#state")[0].innerText = "Waiting for context to start.";
	disableAll();
	d3.json(jobserverURL + "/contexts/kmsc?num-cpu-cores=38&memory-per-node=13g", function(error, success) {
			haveContext = true
			$("#state")[0].innerText = "Ready to resample.";
			showContextRunning();
		})
		.send("POST", "");
}

function stopContext() {
	$("#state")[0].innerText = "Waiting for context to finish.";
	disableAll();
	d3.json(jobserverURL + "/contexts/kmsc", function(error, success) {
			$("#state")[0].innerText = "Please start a context.";
			showContextStopped();
		})
		.send("DELETE");
}

function runSampling() {
	$("#state")[0].innerText = "Resampling, please wait.";
	disableAll();
	d3.json(jobserverURL + "/jobs?appName=km&classPath=spark.jobserver.KMeansExample&context=kmsc", function(error, success) {
			var jobId = success.result.jobId;

			function getResult() {
				setTimeout(function() {
					d3.json(jobserverURL + "/jobs/" + jobId, function(error, success) {
						if (success.status === "RUNNING") {
							getResult();
						} else {
							reloadPageGraphics(success.result);
						}
					})
				}, 5000)
			}
			getResult()
		})
		.send("POST", "");
}

function showContextRunning() {
	$(".enableWhileRunning").prop("disabled", false);
	$(".enableWhileStopped").prop("disabled", true);

}

function showContextStopped() {
	$(".enableWhileRunning").prop("disabled", true);
	$(".enableWhileStopped").prop("disabled", false);

}

function disableAll() {
	$(".enableWhileRunning").prop("disabled", true);
	$(".enableWhileStopped").prop("disabled", true);
}

function isContextRunning() {
	$("#state")[0].innerText = "Syncing with server.";
	d3.json(jobserverURL + "/contexts", function(error, success) {
		if ($.inArray("kmsc", success) !== -1) {
			haveContext = true;
			showContextRunning();
			$("#state")[0].innerText = "Ready to resample.";
		} else {
			haveContext = false;
			showContextStopped();
			$("#state")[0].innerText = "Please start a context.";
		}
	})
}

var haveContext = false;
isContextRunning();