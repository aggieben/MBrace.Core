﻿namespace MBrace.Core.Internals

open System
open System.IO
open System.Net.Http
open System.Net.Http.Headers

/// Seekable HTTP Stream implementation
[<Sealed; AutoSerializable(false)>]
type SeekableHTTPStream(url : string, ?client : HttpClient) =
    inherit Stream()

    let httpClient = defaultArg client (new HttpClient())
    let mutable stream = httpClient.GetStreamAsync(url)
                         |> Async.AwaitTask
                         |> Async.RunSync

    let mutable length : int64 option = None
    let mutable position = 0L
    let mutable isDisposed = false

    let ensureNotDisposed() = 
        if isDisposed then raise <| new ObjectDisposedException("SeekableHTTPStream")

    member self.Url = url

    member self.GetLength() = 
        ensureNotDisposed()
        match length with
        | Some value -> value

    override self.CanRead = ensureNotDisposed(); true
    override self.CanWrite = ensureNotDisposed(); false
    override self.CanSeek = ensureNotDisposed(); true
        | None ->
            use request = new HttpRequestMessage(HttpMethod.Head, url)
            async {
                let! response = httpClient.SendAsync request |> Async.AwaitTask
                length <- response.Content.Headers.ContentLength |> Option.ofNullable
                return length.Value
            } |> Async.RunSync

    override self.Length = self.GetLength()

    override self.Position 
        with get () = position

        and set (value) = 
            ensureNotDisposed()
            stream.Close()

            use request = new HttpRequestMessage()
            request.Headers.Range <- RangeHeaderValue(value, Nullable())

            task {
                let! response = httpClient.SendAsync(request)
                let! _stream = response.Content.ReadAsStreamAsync()
                stream <- _stream
            }
            |> Async.AwaitTask
            |> Async.RunSync

            position <- value

    override self.Seek(offset : int64, origin : SeekOrigin) = 
        ensureNotDisposed()
        let offset = 
            match origin with
            | SeekOrigin.Begin -> offset
            | SeekOrigin.Current -> position + offset
            | _ -> self.Length + offset
        self.Position <- offset
        offset

    override self.Read(buffer : Byte[], offset : Int32, count : Int32) = 
        ensureNotDisposed()
        let n = stream.Read(buffer, offset, count)
        position <- position + int64 n
        n

    override self.SetLength(_ : Int64) = raise <| new NotSupportedException()
    override self.Write(_ : Byte[], _ : Int32, _ : Int32) = raise <| new NotSupportedException()
    override self.Flush() = ensureNotDisposed()

    override self.Close() = 
        if not isDisposed then
            base.Close()
            if stream <> null then
                stream.Close()
            isDisposed <- true

    interface IDisposable with
        member self.Dispose () =
            self.Close()
            httpClient.Dispose()