%%%-------------------------------------------------------------------
%%% @author Tony Wallace <tony@resurrection>
%%% @copyright (C) 2021, Tony Wallace
%%% @doc
%%% Functions:
%%%   1) Reads input from csv_lex and sinks into dets file.
%%%   2) Gathers CSV data into record structures
%%%   3) Maintains correspondence between source document and
%%%      generated record.
%%% To achieve these ends the following data is generated:
%%%   1) Every run will have a run record that maintains
%%%      all metadata about that import.  Each run record
%%%      has a unique id.
%%%   2) Every csv import line results in a 'line' record
%%%      2.1) A csv import line may not correspond to input file line as
%%%           quoted line ends are not counted.
%%%      2.2) A line record contains,
%%%           - the atom line to indicate a line record
%%%           - a run id indicating which run generated it
%%%           - a line id indicating the csv source line
%%%           - a data_valid field that contains a data record
%%%           - an invalid field containing a list of parse tokens
%%%      2.3) A line record will have data in the data_valid field,
%%%           or in the invalid field, but not in both.
%%%   3) The import may have an associated schema which will determine
%%%      the fields and types of data contained in the csv.  When there 
%%%      exists a schema record, and the csv first line contains field 
%%%      headers, then the schema and those headers must match.
%%% Note Version1 will not handle schemas
%%% @end
%%% Created : 28 Apr 2021 by Tony Wallace <tony@resurrection>
%%%-------------------------------------------------------------------
-module(dets_importer).

%% API
-export([import/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
import (SrcPid,Documentation,Options=#{detsfile :=DetsFile}) ->
    %% Open Dets
    %% Save run record in active state
    %% Import the data
    %% Update run record
    %% close Dets
    ok.
%%%===================================================================
%%% Internal functions
%%%===================================================================
-type storage_type() :: [] | {} |#{}.
-type update_fun(storage_type()) -> storage_type(). 
-type test_fun(storage_type()) -> boolean().
-spec while(storage_type(),test_fun(),update_fun()) -> storage_type().
while(S0,FunTest,FunLoop) ->
    case FunTest(S0) of
	true ->
	    while(FunLoop(S0),FunTest,FunLoop);
	false ->
	    S0
    end.
