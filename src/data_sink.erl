%%%-------------------------------------------------------------------
%%% @author Tony Wallace <tony@resurrection>
%%% @copyright (C) 2021, Tony Wallace
%%% @doc
%%%
%%% @end
%%% Created : 25 Apr 2021 by Tony Wallace <tony@resurrection>
%%%-------------------------------------------------------------------
-module(data_sink).

%% API
-export([main/1,sink/1,sink_with_callback/2]).
-define (debug_level,2).
-if (?debug_level==2).
    -define (debug2(Fmt,Data),io:format(Fmt,Data)).
-else.
    -define (debug2(Fmt,Data),ok).
-endif.

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------
main(_) ->
    %% when called as an escript, then copy input to dev/null
    not_implemented.

sink(Src) ->
    sink_with_callback(Src,none).
sink_with_callback(SrcPid,Callback) ->
    MainProc = self(),
    WriterPid = spawn(fun() -> 
			      writer(SrcPid,Callback),
			      MainProc ! eof
		      end),
    ?debug2("Writer proc ~p~n",[WriterPid]),
    WriterPid.


%%%===================================================================
%%% Internal functions
%%%===================================================================
writer(ParserPid,Callback) ->
    ?debug2("Writer calling read~n",[]),
    Token = gen_server:call(ParserPid,read,infinity),
    callback(Callback,Token),
    writer_maybe_exit(Token,ParserPid,Callback).
writer_maybe_exit(eof,_,_) ->
    ?debug2("writer exits~n",[]),
    ok;
writer_maybe_exit(_,ParserPid,CallBack) ->
    writer(ParserPid,CallBack).

callback(none,_) ->
    ok;
callback(Callback,Token) ->
    Callback(Token).
