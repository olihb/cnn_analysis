
// global vars
var chart, input;
var dynamodb
var week = false;
var topic = false;

// call on loading
$(document).ready(function () {
    hash = window.location.hash;
    initializeInterface((hash=="#topic"));
    initializeDynamo();
});


function queryDynamoDB(tag) {
    var input_tag = tag.toLowerCase();
    // retrieve DynamoDB
    dynamodb.getItem({
            TableName:'cnn_2015',
            Key:{
                word:{
                    S:input_tag}
            }
        }, dynamoDBResults
    );
}

function addSeries(result) {
    var tag = result.Item.word.S;
    var data = [];
    var weekOccurrences = {};
    var weekAllOccurrences = {};
    var weekDateArray = [];

    // load date
    $.each(result.Item.dates.L, function (index, value) {
        if (week) {
            var date = new Date(Date.parse(value.S));
            date = getDateOfWeek(date.getWeek(), date.getWeekYear());

            var occurrences = parseFloat(result.Item.occurrences.L[index].N);
            var max_value = parseFloat(result.Item.dates_size.L[index].N);

            if (date in weekOccurrences) {
                weekOccurrences[date] = weekOccurrences[date] + occurrences;
                weekAllOccurrences[date] = weekAllOccurrences[date] + max_value;
            } else {
                weekDateArray.push(date);
                weekOccurrences[date] = occurrences;
                weekAllOccurrences[date] = max_value;
            }

        } else {
            item = [];
            item.push(Date.parse(value.S));
            var occurrences = parseFloat(result.Item.occurrences.L[index].N);
            var max_value = parseFloat(result.Item.dates_size.L[index].N);
            item.push(occurrences / max_value * 100);
            data.push(item);
        }
    });


    if (week) {
        $.each(weekDateArray, function (index, value) {
            item = [];
            item.push(value.getTime());
            var occ = weekOccurrences[value];
            var maxOcc = weekAllOccurrences[value];
            item.push(occ / maxOcc * 100);
            data.push(item);
        });
    }


    chart.addSeries({
        name:tag,
        data:data,
        dataGrouping:{
            enabled:true
        }
    });

}

function removeSeries(tag) {
    var input_tag = tag.toLowerCase();
    var seriesLength = chart.series.length;
    for (var i = seriesLength - 1; i > -1; i--) {
        if (chart.series[i].name == input_tag)
            chart.series[i].remove();
    }
    if (chart.series.length == 0) {
        chart.showLoading("Type in a word in the input field at the top of the screen to visualize its trend")
    }
}

function dynamoDBResults(err, data) {

    // verify for errors
    if (err == true || (data != true && jQuery.isEmptyObject(data))) {

        // invalid input - trigger error feedback
        $('input.tm-input')
            .attr('placeholder', 'Word Not Found')
            .css('border-color', 'red')
            .delay(1500)
            .queue(function (next) {
                $(this).attr('placeholder', 'Type words to chart');
                $(this).css('border-color', 'white');
                input.tagsManager('popTag');
                next();
            });
    } else {
        // valid input - add series to chart
        chart.hideLoading();
        addSeries(data);
    }
}

function initializeDynamo() {
    // initialize connector to AWS DynamoDB - read-only access to the cnn table
    dynamodb = new AWS.DynamoDB({
        accessKeyId:"AKIAIXQEGMSMVFJD32YA",
        secretAccessKey:"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",
        region:"us-east-1"});
}

function week_button_callback() {

    // update week global state
    week = $("#toggle_week").is(':checked');

    tag_list = [];
    if (topic) {
        tag_list = $(".chosen-select").val()
    } else {
        tag_list = jQuery(".tm-input").tagsManager('tags');
    }

    while (chart.series.length > 0)
        chart.series[0].remove(true);

    tag_list.forEach(function (entry) {
        queryDynamoDB(entry);
    });
}

function initializeInterface(topics) {


    if (topics) {
        // initialize topics

        topic=true;
        $("#input_tags").hide();
        select_width = $("#container_input").width();
        $("#input_topics").css("width", select_width * 0.75);
        $(".chosen-select").chosen();
        $.get("data/topics.json", function (data) {
            data.forEach(function (entry) {
                $(".chosen-select").append($('<option>', { value:entry.key }).text(entry.name));
            });
            $(".chosen-select").trigger('chosen:updated');
        });
        $(".chosen-select").on('change', function(evt, params) {
            if ("selected" in params){
                queryDynamoDB(params["selected"]);
            } else {
                removeSeries(params["deselected"]);
            }
        });

    } else {
        // initialize tags

        $("#input_topics").hide();
        jQuery(".tm-input").tagsManager();
        jQuery(".tm-input").on('tm:pushed', function (e, tag) {
            queryDynamoDB(tag);
        });
        jQuery(".tm-input").on('tm:spliced', function (e, tag) {
            removeSeries(tag);
        });
        input = $('.tm-input').tagsManager();
        jQuery(".tm-input").focus();
    }

    // initialize chart
    $('.viz-chart').highcharts("StockChart", {
        chart:{
            zoomType:'x',
            showAxes:true
        },
        title:{
            text:null
        },
        loading:{

        },
        credit:{
            enabled:false
        },
        subtitle:{
            text:document.ontouchstart === undefined ?
                'Click and drag in the plot area to zoom in' : 'Pinch the chart to zoom in'
        },
        xAxis:{
            type:'datetime',
            min:Date.UTC(2000, 1, 1),
            max:Date.UTC(2015, 1, 1)
        },
        yAxis:{
            title:{
                text:'% of occurrences'
            },
            min:0
        },
        legend:{
            enabled:true
        },
        tooltip: {
            valueDecimals: 2,
            valueSuffix: ' %'
        }
    });
    chart = $('.viz-chart').highcharts();
    chart.showLoading("Type in a word in the input field at the top of the screen to visualize its trend")

    // initialize week button
    $('#toggle_week').click(function () {
        week_button_callback();
    });
}

