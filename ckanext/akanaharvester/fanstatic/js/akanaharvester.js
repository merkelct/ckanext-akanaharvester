/**
 * Created by merkelct on 4/24/17.
 */

$(document).ready(function () {
    if ($('input[name="source_type"]:checked').val() === 'akana') {
        $('#field-config').prop('readonly', true);


        $('#field-private').change(function () {
            updateConfigs()
        });
        $('#field-tags').change(function () {
            updateConfigs()
        });
        $('#field-grps').change(function () {
            updateConfigs()
        });
         $('#field-swagger').change(function () {
            updateConfigs()
        });
    }

    $('input[name="source_type"]').attr("onClick", "disableConfigs()");
});

function disableConfigs() {

    if ($('input[name="source_type"]:checked').val() === 'akana') {
        $('#fiel-config').prop('readonly', true);


    } else {
        $('#field-config').prop('readonly', false);
    }

}


function updateConfigs() {
    var configs = {};

    configs["isprivate"] = $('#field-private').val();
    configs["swagger"] = $('#field-swagger').val();

    // for ( var tags in $('#field-tags').val():
    var tagarr = $('#field-tags').val().split(',');
    var grparr = $('#field-grps').val().split(',');
    var tags = []

    for (var i = 0; i < tagarr.length; i++) {
        tags.push({'name': tagarr[i]});

    }
    configs['default_tags'] = tags;
    var grps = []
    for (var i = 0; i < grparr.length; i++) {
        grps.push(grparr[i]);

    }
    configs['default_groups'] = grps;
    console.dir(JSON.stringify(configs));

    $("#field-config").text(JSON.stringify(configs));

}


$("source-new").submit(function () {
    $('#field-private').remove();
    $('#field-tags').remove();
    $('#field-grps').remove();
});
