package software.amazon.awssdk.http.nio.netty.internal.utils;

import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;
import java.util.function.Consumer;
import software.amazon.awssdk.utils.Logger;

/**
 * An implementation of {@link ChannelHandlerContext} that ensures all writes are performed in the order they are invoked.
 *
 * This works around https://github.com/netty/netty/issues/7783 where writes by an event loop 'skip ahead' of writes off of the
 * event loop.
 */
public class OrderedWriteChannelHandlerContext extends DelegatingChannelHandlerContext {
    private static final AttributeKey<Void> ORDERED = AttributeKey.newInstance("OrderedWriteChannelHandlerContext.ORDERED");

    public static ChannelHandlerContext wrap(ChannelHandlerContext ctx) {
        if (ctx.channel().hasAttr(ORDERED)) {
            return ctx;
        }
        return new OrderedWriteChannelHandlerContext(ctx);
    }

    private OrderedWriteChannelHandlerContext(ChannelHandlerContext delegate) {
        super(delegate);
        delegate.channel().attr(ORDERED).set(null);
    }

    @Override
    public ChannelFuture write(Object msg) {
        return doInOrder(promise -> super.write(msg, promise));
    }

    @Override
    public ChannelFuture write(Object msg, ChannelPromise promise) {
        doInOrder(() -> super.write(msg, promise));
        return promise;
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg) {
        return doInOrder(promise -> super.writeAndFlush(msg, promise));
    }

    @Override
    public ChannelFuture writeAndFlush(Object msg, ChannelPromise promise) {
        doInOrder(() -> super.writeAndFlush(msg, promise));
        return promise;
    }

    private ChannelFuture doInOrder(Consumer<ChannelPromise> task) {
        ChannelPromise promise = channel().newPromise();
        if (!channel().eventLoop().inEventLoop()) {
            task.accept(promise);
        } else {
            // If we're in the event loop, queue a task to perform the write, so that it occurs after writes that were scheduled
            // off of the event loop.
            channel().eventLoop().execute(() -> task.accept(promise));
        }
        return promise;
    }

    private void doInOrder(Runnable task) {
        if (!channel().eventLoop().inEventLoop()) {
            task.run();
        } else {
            // If we're in the event loop, queue a task to perform the write, so that it occurs after writes that were scheduled
            // off of the event loop.
            channel().eventLoop().execute(task);
        }
    }
}
