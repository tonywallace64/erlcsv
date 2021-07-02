%%%-------------------------------------------------------------------
%%% @author Tony Wallace <tony@resurrection>
%%% @copyright (C) 2021, Tony Wallace
%%% @doc
%%% An interactive test module for csv_lex
%%% @end
%%% Created : 19 Apr 2021 by Tony Wallace <tony@resurrection>
%%%-------------------------------------------------------------------
-module(csv_lex_test).

-define (debug_level,2).
-if (?debug_level==2).
    -define (debug2(Fmt,Data),io:format(Fmt,Data)).
-else.
    -define (debug2(Fmt,Data),ok).
-endif.
		
%% API
-export([start/1,source/0,sandpit/0,select_file/1]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
start(DateFormat) ->
    process_flag(trap_exit, true),
    SrcPid = spawn(fun source/0),
    CsvPid = csv_process(#{input => SrcPid,
			   date_format => DateFormat}),
    ?debug2("Src proc ~p~n",[SrcPid]),
    _Writer = data_sink:sink(CsvPid),
    await_exit().
select_file(DateFormat) ->
    FN = file_chooser:get(),
    SrcPid = file_reader:readfile(FN,[]),
    CsvPid = csv_process(#{input => SrcPid,
			   date_format => DateFormat,
			   source_file => FN}),
    _Writer = data_sink:sink(CsvPid),
    await_exit().
    

sandpit() ->
    SrcPid = spawn(fun source/0),
    sandpit2(SrcPid).

sandpit2(SrcPid) ->
    Result = gen_server:call(SrcPid,read,infinity),
    ?debug2("gen_server call returns ~p~n",[Result]),
    case Result == eof of
	true ->
	    ok;
	false ->
	    sandpit2(SrcPid)
    end.

csv_process(M0=#{input := _SrcPid, date_format := _DateFormat}) ->
    {ok,ParserPid} = csv_lex:start_link(M0#{
					 read_timeout => infinity},
					[{debug,[trace]}]),
    ?debug2("Parser proc ~p~n",[ParserPid]),
    ParserPid.




%%%===================================================================
%%% Internal functions
%%%===================================================================
source() ->
    %% reads keyboard input in response to a read request
    Eof =
	receive 
	    {'$gen_call',From,read} ->
		?debug2("csv_lex_test:source read request from ~p~n",[From]),
		{ok,Term} = io:fread("csv_lex_test:source> ","~s"),
		{Eof1,ExpandedTerm} = expand(Term),
		gen_server:reply(From,ExpandedTerm),
		Eof1;
	    X -> ?debug2("Unexpected message ~p~n",X),
		 false
	end,
    maybe_eof(Eof).

maybe_eof(false) ->
    source();
maybe_eof(true) ->
    receive 
	{'$gen_call',From,read} ->
	    gen_server:reply(From,eof)
    end,
    ok.

expand(InputString) ->
    SupStr =
	lists:foldl(
	  fun({SearchString,Repl},A) ->
		  lists:flatten(string:replace(A,SearchString,Repl,all))
	  end,
	  InputString,[{"<cr>",[13]},{"<lf>",[10]},{"<eof>",[4]}]),
    ST = [ReturnText|_] =
	string:split(SupStr,[4]),
    ContainsEof = not (length(ST)=:=1),
    {ContainsEof,ReturnText}.

await_exit() ->    
    receive 
	X -> 
	    ?debug2("~p~n",[X])
    end.

