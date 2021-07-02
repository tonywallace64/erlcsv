%%%-------------------------------------------------------------------
%%% @author Tony Wallace <tony@resurrection>
%%% @copyright (C) 2021, Tony Wallace
%%% @doc
%%% Reads a file source for the lexer

%%% @end
%%% Created : 24 Apr 2021 by Tony Wallace <tony@resurrection>
%%%-------------------------------------------------------------------
-module(file_reader).

%% API
-export([readfile/2]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------

-define (read(Fn),io:get_line(Fn,"")).
-spec readfile(file:filename()|none,list()) -> {ok, Pid :: pid()} |
		      {error, Error :: {already_started, pid()}} |
		      {error, Error :: term()} |
		      ignore.
readfile(none,_) ->
    {error,no_filename};
readfile(Filename,_ReadOpts) ->
    MainProc = self(),
    spawn(
      fun() ->
	      io:format("Opening file for read ~p~n",[Filename]),
	      {ok,SrcFile} = file:open(Filename,[read]),
	      loop(?read(SrcFile),SrcFile),
	      MainProc ! exit
      end).

loop(eof,SrcFile) ->
    io:format("File reader pid=~p at eof~n",[self()]),
    file:close(SrcFile),
    receive 
	{'$gen_call',From,read} ->
	    gen_server:reply(From,eof)
    end;
loop(Str,SrcFile) ->
    receive 
	{'$gen_call',From,read} ->
	    gen_server:reply(From,Str),
	    loop(?read(SrcFile),SrcFile);
	X ->
	    io:format("file_reader unexpected message ~p~nexiting....~n",[X])
    end.

