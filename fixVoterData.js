'use strict';

const fs = require('fs');
const csv = require('csv');
const moment = require('moment');

const inputColumnNames = [
  "text_name_first",
  "text_name_middle",
  "text_name_last",
  "cde_gender",
  "text_registrant_id",
  "text_res_address_nbr",
  "text_res_address_nbr_suffix",
  "cde_street_dir_prefix",
  "text_street_name",
  "cde_street_type",
  "cde_street_dir_suffix",
  "cde_res_unit_type",
  "text_res_unit_nbr",
  "text_res_city",
  "cde_res_state",
  "text_res_zip5",
  "precinct_part_text_name",
  "precinct_text_name",
  "date_of_birth",
  "date_of_registration",
  "text_phone_area_code",
  "text_phone_exchange",
  "text_phone_last_four",
  "text_election_code_1",
  "text_election_code_2",
  "text_election_code_3",
  "text_election_code_4",
  "text_election_code_5",
  "text_election_code_6",
  "text_election_code_7",
  "text_election_code_8",
  "text_election_code_9",
  "text_election_code_10",
  "cde_party",
];

const outputColumnNames = [
  "firstName",
  "middleName",
  "lastName",
  "gender",
  "registrantIt",
  "address1",
  "address2",
  "city",
  "state",
  "zipcode",
  "precinctId",
  "precinctName",
  "dateOfBirth",
  "dateOfRegistration",
  "homePhone",
  "elections",
  "party"
];

function saveColumnNames(newColumnNames) {
  console.log(newColumnNames);
  //columnNames = newColumnNames;
  return newColumnNames;
}

const parser = csv.parse({
  delimiter: ',',
  header: true,
  columns: saveColumnNames,
  ltrim: true,
  rtrim: true,
  trim: true
});

const input = fs.createReadStream('d3-transform1.csv');

const transform = csv.transform(function(row) {
  //console.log("input=", row);

  const newRow = {};

  newRow.firstName = row.text_name_first;
  newRow.middleName = row.text_name_middle;
  newRow.lastName = row.text_name_last;
  newRow.gender = row.cde_gender;
  newRow.registrantIt = row.text_registrant_id;
  
  let aParts = [
    row.text_res_address_nbr,
    row.text_res_address_nbr_suffix,
    row.cde_street_dir_prefix,
    row.text_street_name,
    row.cde_street_type,
    row.cde_street_dir_suffix
    ];
    aParts = aParts.filter(part =>
      aParts != undefined && part != null && part.length > 1); 
  newRow.address1 = aParts.join(' ').trim();
  
  newRow.address2 = row.cde_res_unit_type + row.text_res_unit_nbr,
  newRow.city = row.text_res_city;
  newRow.state = row.cde_res_state;
  newRow.zipcode = row.text_res_zip5;
  newRow.precinctId = row.precinct_part_text_name;
  newRow.precinctName = row.precinct_text_name;
  newRow.dateOfBirth = moment(row.date_of_birth).format('YYYY-MM-DD');
  newRow.dateOfRegistration = moment(row.date_of_registration).format('YYYY-MM-DD');
  newRow.homePhone = (row.text_phone_area_code
    + ' ' + row.text_phone_exchange
    + ' ' + row.text_phone_last_four).trim();
  
  let eCodes = [
    row.text_election_code_1,
    row.text_election_code_2,
    row.text_election_code_3,
    row.text_election_code_4,
    row.text_election_code_5,
    row.text_election_code_6,
    row.text_election_code_7,
    row.text_election_code_8,
    row.text_election_code_9,
    row.text_election_code_10
  ];
  eCodes = eCodes.filter(part =>
    eCodes != undefined && part != null && part.length > 1); 
  newRow.elections = eCodes.join(',').trim();

  newRow.party = row.cde_party;

  //console.log("output=", newRow);
  return newRow;
  });

const output = fs.createWriteStream('d3-transform2.csv');

input.pipe(parser)
  .pipe(transform)
  .pipe(csv.stringify({columns: outputColumnNames, header: true}))
  .pipe(output);

