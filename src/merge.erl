%%%-------------------------------------------------------------------
%%% @author Tony Wallace <tony@resurrection>
%%% @copyright (C) 2021, Tony Wallace
%%% @doc
%%% On read this module returns a single merged record from all input
%%% sources.
%%% @end
%%% Created :  2 May 2021 by Tony Wallace <tony@resurrection>
%%%-------------------------------------------------------------------

% --------------------------------------------------------------------
% Technical notes:
% Every input file becomes an entry in a heap.  The heap contains
% the last record read from that file.
-module(merge).

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
-spec start_link(list(),list()) -> {ok, Pid :: pid()} |
		      {error, Error :: {already_started, pid()}} |
		      {error, Error :: term()} |
		      ignore.
start_link(ImportFileList,Opts) ->
    gen_server:start_link(?MODULE, ImportFileList, Opts).

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
init([]) ->
    {stop,normal,#{}};
init(ImportFileList) ->
    process_flag(trap_exit, true),
    FileSeqs = lists:zip(lists:seq(1,length(ImportFileList)),ImportFileList),
    %% Organise a reader for each input file
    %% Pipe that reader into csv_lex
    %% Accumulate tokens into lines
    %% The first line of the csv file contains column names.  
    %% Extract these into a list.
    %% a reader/buffer pair.
    ReaderList =
	[begin 
	     ReaderPid = file_reader:readfile(Filename,[]),
	     {ok,LexerPid}  = csv_lex:start_link(
				#{input => ReaderPid,
				  date_format => "MM/DD/YYYY",
				  read_timeout => 1000},[]),
	     {ok,LinesPid} = line_accumulator:start_link(
			       #{input => LexerPid, mode => tuple},[debug]),
	     {0,Cols} = gen_server:call(LinesPid,read),
	     case gen_server:call(LinesPid,read) of
		 eof ->
		     eof;
		 {Line,DataRec} -> 

		     Key = element(KeyPos,DataRec),
		     {Key,#{source => LinesPid,
			    source_id => SrcId,
			    data => DataRec,
			    cols => Cols,
			    line => Line,
			    filename => Filename,
			    keypos => KeyPos}}
	     end
	 end || {SrcId,{Filename,KeyPos}} <- FileSeqs],
    %% Now form these key value pairs into a heap
    %% (unless eof), so filter these out before making heap

    RL2 = [{K,V} || {K,V} <- ReaderList],
    H0 = heaps:from_list(RL2),
    %% exposing the heap minimum allows comparision of current_rec and
    %% peek during pattern matching
    State = #{heap => H0, 
	      file_import_list => ImportFileList},
    {ok, State}.

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
handle_call(state, _From,State) ->
    {reply,State,State};
handle_call(read, _From, 
	    State = #{heap := H0, 
		      file_import_list :=FIL}) ->
    %% the heap minimum contains the key and data to process.
    case heaps:empty(H0) of
	true ->
	    {stop,normal,eof,State};
	false ->
	    Acc0 = erlang:make_tuple(length(FIL),null),
	    {CurrentKey,_} = heaps:min(H0),
	    {Reply,H1} = merge(CurrentKey,CurrentKey,H0,Acc0),
	    {reply, Reply, State#{heap := H1}}
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
handle_cast(exit, State) ->
    {stop,normal,State}.

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
%% Merge loops while the minimum key in the heap is equal to the 
%% key being processed.

merge(Key,Key,H0,Acc0) ->
    %% add current record to acc
    {H1,Acc1} = heap_item_to_acc({H0,Acc0}),
    {PeekKey,_} = heaps:min(H1),
    merge(Key,PeekKey,H1,Acc1);
merge(_,_,Heap,Acc) ->
    {Acc,Heap}.

read_heap(Heap) ->  
    {heaps:min(Heap),heaps:delete_min(Heap)}.


update_heap(RS=#{source:=SrcPid},Heap) ->
    update_heap(gen_server:call(SrcPid,read),RS,Heap).

update_heap(eof,_RS,Heap) ->
    %% if an input stream returns eof, do not put
    %% it back in the heap
    Heap;
update_heap({Line,DataRec},
	    RS=#{keypos := KeyPos},Heap) ->
    %% a read of the input stream has resulted in data,
    %% add the buffer back into the heap
    Key = element(KeyPos,DataRec),
    RS2=RS#{data := DataRec, line:=Line},
    heaps:add({Key,RS2},Heap).

%% heap_item_to_acc
%% 1) reads the current minimum from the heap,
%%    removing that item from the heap
%% 2) Copies the data into the accumulator in its correct position
%% 3) updates the heap with new data
heap_item_to_acc({H0,Acc0}) ->
    {{_Key,Buff},H1} = read_heap(H0),
    #{source_id := SrcId,
      data := Data} = Buff,
    H2 = update_heap(Buff,H1),
    Acc1 = erlang:setelement(SrcId,Acc0,Data),
    {H2,Acc1}.

test() ->
    DataDir = "/home/tony/Projects/vaers/data",
    Set1 = filename:join([DataDir,"as_of_2021-04-09"]),
    Set2 = filename:join([DataDir,"as_of_2021-04-16","2021VAERSData"]),
    Files1 = [filename:join([Set1,File]) || File <- filelib:wildcard("*.csv",Set1)],
    Files2 = [filename:join([Set2,File]) || File <- filelib:wildcard("*.csv",Set2)],
    %% all these files have key in position 1
    FilesWithKeypos = [{File,1} || File <- Files1 ++ Files2 ],
    {ok,Pid}=merge:start_link(FilesWithKeypos,[debug]),
    test_loop(Pid).

test_loop(Pid) ->
    test_handle_input(assert_ok(io:fread("merge> ","~s")),Pid).

test_handle_input(["read"],Pid) ->
    MR = gen_server:call(Pid,read),
    io:format("~p~n",[MR]),
    test_loop(Pid);
test_handle_input(["exit"],Pid) ->
    gen_server:cast(Pid,exit),
    ok;
test_handle_input(["state"],Pid) ->
    R=gen_server:call(Pid,state),
    io:format("~p~n",[R]),
    ok;
test_handle_input(UnexpectedInput,Pid) ->
    io:format("Unexpected: ~p~n",[UnexpectedInput]),
    io:format("Valid commands are \"read\", \"state\" and \"exit\" ~n",[]),
    test_loop(Pid).

assert_ok({ok,V}) ->
    V;
assert_ok(X) ->
    throw (X).
