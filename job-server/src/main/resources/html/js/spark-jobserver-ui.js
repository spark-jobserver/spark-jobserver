function showJobs(filter,$tableBody) {
    $.getJSON(
        'jobs',
        filter,
        function(jobs) {
            $tableBody.html("");
            var jobsHtml = "";
            $.each(jobs, function(key, job) {
                jobsHtml += "<tr>";
                jobsHtml += "<td><a href='./jobs/" + job.jobId + "'>" + job.jobId + "</a> (<a href='./jobs/" + job.jobId + "/config'>C</a>)</td>";
                jobsHtml += "<td>" + job.classPath + "</td>";
                jobsHtml += "<td>" + job.context + "</td>";
                jobsHtml += "<td>" + job.startTime + "</td>";
                jobsHtml += "<td>" + job.duration + "</td>";
                jobsHtml += "</tr>";
            });
            $tableBody.html(jobsHtml);
        });
}

function getJobs() {
    //show error jobs
    showJobs({status:"error"},$('#failedJobsTable > tbody:last'));
    //show running jobs
    showJobs({status:"running"},$('#runningJobsTable > tbody:last'));
    //show complete jobs
    showJobs({status:"finished"},$('#completedJobsTable > tbody:last'));
}

function getContexts() {
    $.getJSON(
        'contexts',
        '',
        function(contexts) {
            $('#contextsTable tbody').empty();

            $.each(contexts, function(key, contextName) {
                var items = [];
                items.push("<tr><td>" + contextName + "</td></tr>");
                $('#contextsTable > tbody:last').append(items.join(""));
            });
        });
}

function getJars() {
    $.getJSON(
        'jars',
        '',
        function(jars) {
            $('#jarsTable tbody').empty();

            $.each(jars, function(jarName, deploymentTime) {
                var items = [];
                items.push("<tr>");
                items.push("<td>" + jarName + "</td>");
                items.push("<td>" + deploymentTime + "</td>");
                items.push("</tr>");
                $('#jarsTable > tbody:last').append(items.join(""));
            });
        });
}


$(function () {
    $('#navTabs a[data-toggle="tab"]').on('show.bs.tab', function (e) {
        var target = $(e.target).attr("href");

        if (target == '#jobs') {
            getJobs();
        } else if (target == "#contexts") {
            getContexts();
        } else {
            getJars();
        }
    })
    getJobs();
});
