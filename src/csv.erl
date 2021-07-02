%%%-------------------------------------------------------------------
%%% @author Tony Wallace <tony@resurrection>
%%% @copyright (C) 2021, Tony Wallace
%%% @doc
%%% Tool for processing a csv file.  
%%% @end
%%% Created : 28 May 2021 by Tony Wallace <tony@resurrection>
%%%-------------------------------------------------------------------
-module(csv).

%% API
-export([process_file/3,test/0]).
-type user_state() :: term().
-type update_fun_reply() :: user_state() | exit.
-type update_fun() :: fun(({header,[string()]}|{data,[term()]},user_state()) -> 
				 user_state() | exit).
-export_type ([user_state/0,update_fun/0,update_fun_reply/0]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%%% @doc
%%% SrcFile is a path to the csv file to process
%%% UpdateFun is a callback function.  Which maintains state for
%%% the process calling process_file.  This state is first set up
%%% as the third parameter to the call.  The first call to UpdateFun
%%% has the form (header,[string()],InitialState) and lists the header data from
%%% the csv.  Subsequent calls are of the form ({data,tuple()},SavedState) where
%%% tuple represents a single record from the csv.
%%% The UpdateFun returns either a new state which will be returned
%%% to it in the next call, or the atom exit, which cause the process_file
%%% call to finish.
%%% Note that the csv data line is returned as a list rather than a tuple.
%%% This is intentional and allows the final form, be it tuple, record or
%%% to be decided by the user of the data.
%% @spec
%% @end
%%--------------------------------------------------------------------
-spec process_file(file:name_all(),update_fun(),user_state()) -> ok.
process_file(SrcFile,UpdateFun,InitialUserState) ->
    io:format("~p process_file(~p,~p,~p)~n",
	      [self(),SrcFile,UpdateFun,InitialUserState]),
    SrcPid = file_reader:readfile(SrcFile,[]),
    {ok,CsvPid} =  csv_lex:start_link(#{input => SrcPid,
					date_format => "MM/DD/YYYY",
					read_timeout => 5000},[]),
				     % [{debug,[trace]}]),
    {ok,AccPid} = line_accumulator:start_link(#{input => CsvPid, mode => raw},[]),
    
    {0,FirstLine} = gen_server:call(AccPid,read),
    Tokens = [TK || #{token := TK} <- FirstLine],
    ColNames = [X || {string,X} <- Tokens],
    UserState1 = UpdateFun({header,ColNames},InitialUserState),

    LineProcessingFun = make_line_processor(UpdateFun),
    InputLine = gen_server:call(AccPid,read),
    
    WhileResult =
	while(
	  #{input => AccPid, input_line => InputLine, 
	    columns => ColNames,
	    user_state => UserState1},
	  LineProcessingFun,
	  fun is_eof_or_exit/1),
    #{user_state := US} = WhileResult,
    US.
    
%%%===================================================================
%%% Internal functions
%%%===================================================================
make_line_processor(UpdateFun) ->
    fun(S0=#{input := AccPid, 
	     columns := Columns,
	     input_line := InputLine,
	     user_state := UserState0}) ->
	    {_LineNo,D0} = InputLine,
	    S1 = 
		case (length(D0) == length(Columns)) of
		    true ->
			%% Extract tokens from lex stuff
			Tokens = [Tk || #{token := Tk} <- D0],
			%% Following expression needs to handle null fields
			D1 = [case X of 
				  {_,V} -> V; 
				  _ -> X end || X <- Tokens],
			UserState1 = UpdateFun({data,D1},UserState0),
			S0#{user_state := UserState1};
		    false ->
			%throw({unexpected_number_of_fields,{line,LineNo},{fields,length(D0)}})
			S0
		end,
	    S1#{input_line := gen_server:call(AccPid,read)}
    end.



is_eof(#{input_line := eof}) ->
    true;
is_eof(_) ->
    false.

is_eof_or_exit(#{user_state := exit}) ->
    true;
is_eof_or_exit(S) ->
    is_eof(S).

while(State,UpdateFun,ExitTest) ->
    case ExitTest(State) of
	true ->
	    State;
	false ->
	    while(UpdateFun(State),UpdateFun,ExitTest)
    end.

test() ->
    File = file_chooser:get(),
    io:format("Reading file ~p~n",[File]),
    io:format("Enter a number to read that number of lines~n"),
    io:format("Enter a negative number to read to end of file~n"),
    io:format("Enter a string to exit~n"),
    UpdateFun =
	fun({data,LineData},0) ->
		case io:fread("Lines to get> ","~d") of
		    {ok,[Lines]} -> 
			io:format("~p~n",[LineData]),
			Lines - 1;
		    _ ->
			exit
		end;
	   ({data,LineData},N) ->
		io:format("~p~n",[LineData]),
		N-1;
	   ({header,Header},N) ->
		io:format("Header ~p~n",[Header]),
		N
	end,
    process_file(File,UpdateFun,0).
			 
			
