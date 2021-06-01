using MessagePack;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbiqMQServer
{
    [MessagePackObject]
    public class MessageModel
    {
        [Key(0)]
        public string Message { get; set; }

        [Key(1)]
        public int MessageLength { get; set; }

        public MessageModel(string message, int length)
        {
            Message = message;
            MessageLength = length;
        }

    }
}
