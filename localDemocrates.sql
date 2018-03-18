use mikeForKansas;

select firstName,lastName, address1, zipcode, precinctId, gender, homePhone, dateOfBirth, party, elections, elections like "%PR%"
	from d3Voters
    where (precinctId like 'N601%' or precinctId like 'N602%') and (party="D")
    order by lastName, firstName;