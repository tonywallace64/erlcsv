%%%-------------------------------------------------------------------
%%% @author Tony Wallace <tony@resurrection>
%%% @copyright (C) 2021, Tony Wallace
%%% @doc
%%% repeatedly read lex_csv to accumulate data
%%% on a linewise basis
%%% @end
%%% Created : 28 Apr 2021 by Tony Wallace <tony@resurrection>
%%%-------------------------------------------------------------------
-module(line_accumulator).

-behaviour(gen_server).

%% API
-export([start_link/2,test/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3, format_status/2]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%% @end
%%--------------------------------------------------------------------

-spec start_link(#{},list()) -> {ok, Pid :: pid()} |
		      {error, Error :: {already_started, pid()}} |
		      {error, Error :: term()} |
		      ignore.
start_link(I=#{input := _ParserPid, mode := _},Opts) ->
    gen_server:start_link( ?MODULE, I, Opts).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: term()) -> {ok, State :: term()} |
			      {ok, State :: term(), Timeout :: timeout()} |
			      {ok, State :: term(), hibernate} |
			      {stop, Reason :: term()} |
			      ignore.
init(State) ->
    process_flag(trap_exit, true),
    {ok, State#{token_list => []}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%% @end
%%--------------------------------------------------------------------
-spec handle_call(Request :: term(), From :: {pid(), term()}, State :: term()) ->
			 {reply, Reply :: term(), NewState :: term()} |
			 {reply, Reply :: term(), NewState :: term(), Timeout :: timeout()} |
			 {reply, Reply :: term(), NewState :: term(), hibernate} |
			 {noreply, NewState :: term()} |
			 {noreply, NewState :: term(), Timeout :: timeout()} |
			 {noreply, NewState :: term(), hibernate} |
			 {stop, Reason :: term(), Reply :: term(), NewState :: term()} |
			 {stop, Reason :: term(), NewState :: term()}.
handle_call(read, _From, State=#{input := Lexer, mode:=raw}) ->
    assemble_line(gen_server:call(Lexer,read),State);
handle_call(read, _From, State=#{input := Lexer, mode:=tuple}) ->
    case assemble_line(gen_server:call(Lexer,read),State) of
	{reply,{LineNo,TokenList},S1} ->
	    NewReply = {LineNo,form_tuple(TokenList)},
	    {reply,NewReply,S1};
	StopMsg ->
	    StopMsg
    end.
      

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_cast(Request :: term(), State :: term()) ->
			 {noreply, NewState :: term()} |
			 {noreply, NewState :: term(), Timeout :: timeout()} |
			 {noreply, NewState :: term(), hibernate} |
			 {stop, Reason :: term(), NewState :: term()}.
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
-spec handle_info(Info :: timeout() | term(), State :: term()) ->
			 {noreply, NewState :: term()} |
			 {noreply, NewState :: term(), Timeout :: timeout()} |
			 {noreply, NewState :: term(), hibernate} |
			 {stop, Reason :: normal | term(), NewState :: term()}.
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%% @end
%%--------------------------------------------------------------------
-spec terminate(Reason :: normal | shutdown | {shutdown, term()} | term(),
		State :: term()) -> any().
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%% @end
%%--------------------------------------------------------------------
-spec code_change(OldVsn :: term() | {down, term()},
		  State :: term(),
		  Extra :: term()) -> {ok, NewState :: term()} |
				      {error, Reason :: term()}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for changing the form and appearance
%% of gen_server status when it is returned from sys:get_status/1,2
%% or when it appears in termination error logs.
%% @end
%%--------------------------------------------------------------------
-spec format_status(Opt :: normal | terminate,
		    Status :: list()) -> Status :: term().
format_status(_Opt, Status) ->
    Status.

%%%===================================================================
%%% Internal functions
%%%===================================================================
assemble_line(eof,S) ->
    {stop,normal,eof,S};
assemble_line({line,N},
	      State=#{token_list := Tokens}) ->
    Reply = {N,lists:flatten(Tokens)},
    NS = State#{token_list := []},
    {reply,Reply,NS};
assemble_line(Token,
	      State=#{input := Parser, token_list := Tokens}) ->
    assemble_line(gen_server:call(Parser,read),
		  State#{token_list := [Tokens,Token]}).

form_tuple(Tokens) ->
    %% Following expression needs to handle null fields
    D1 = [case  X of {_,V} -> V; _ -> X end || #{token :=X} <- Tokens],
    list_to_tuple(D1).


test() ->
    test(raw),
    test(tuple).

test(Mode) ->
    io:format("test starting with call ~p src pid=",[Mode]),
    SrcPid = start_source(),
    io:format("~p~nStarting line aggregator pid=",[SrcPid]),
    {ok,AccPid} = start_link(#{input => SrcPid, mode => Mode},[]),
    io:format("~p~n",[AccPid]),
    Lines = [gen_server:call(AccPid, read),
	     gen_server:call(AccPid, read),
	     gen_server:call(AccPid, read),
	     gen_server:call(AccPid, read)],	     
    io:format("~p ~p~n",[Mode,Lines]).
    
start_source() ->
    spawn(
      fun() ->
	      ExitTest = 
		  fun([]) -> true;
		     (L) when is_list(L) -> false;
		     (X) ->
			  throw({exit_test_called_with_unexpected_value,X})
		  end,
	      UpdateFun = 
		  fun(S0=[H|T]) ->
			  receive 
			      {'$gen_call',From,read} ->
				  gen_server:reply(From,H),
				  T;
			      X ->
				  io:format("unexpected message ~p~n",[X]),
				  S0
			  end;
		     (X) ->
			  throw({update_fun_called_with_unexpected_value,X})
	  
		  end,
	      repeat(test_source_data(),UpdateFun,ExitTest)
      end).
		  
test_source_data() ->
    [#{token => {string,"NAME"}},
     #{token => {string,"COLOUR"}},
     {line,0},
     #{token => {string,"Tony"}},
     #{token => {string,"Red"}},{line,1},
     #{token => {string,"Rose"}},
     #{token => {string,"Blue"}},{line,2},eof].


%% reapeat update on state until ExitTest
repeat(State,Update,ExitTest) ->
    S1 = Update(State),
    case ExitTest(S1) of
	false ->
	    repeat(S1,Update,ExitTest);
	true ->
	    S1
    end.

while(State,Test,Update) ->
    case Test(State) of 
	true ->
	    while(Update(State),Test,Update);
	false ->
	    State
    end.
